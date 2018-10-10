package ch.fhnw.edu.rental.persistence.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import ch.fhnw.edu.rental.model.Movie;
import ch.fhnw.edu.rental.persistence.MovieRepository;
import ch.fhnw.edu.rental.persistence.PriceCategoryRepository;

@Component
public class MovieRepositoryImpl implements MovieRepository {

    private final JdbcTemplate jdbcTemplate;
    private final PriceCategoryRepository priceCategoryRepo;

    public MovieRepositoryImpl(JdbcTemplate jdbcTemplate, PriceCategoryRepository priceCategoryRepo) {
        this.jdbcTemplate = jdbcTemplate;
        this.priceCategoryRepo = priceCategoryRepo;
    }

    @Override
    public Optional<Movie> findById(Long id) {

        List<Movie> movies = jdbcTemplate.query(
            "select * from MOVIES where MOVIE_ID = ?",
            (rs, row) -> createMovie(rs),
            id
        );

        if (movies.size() == 0) {
            return Optional.empty();
        } else if (movies.size() > 1) {
            throw new IllegalStateException("Multiple movies have the same id");
        } else {
            return Optional.of(movies.get(0));
        }
    }

    @Override
    public List<Movie> findAll() {
        return jdbcTemplate.query(
            "select * from MOVIES",
            (rs, row) -> createMovie(rs)
        );
    }

    @Override
    public List<Movie> findByTitle(String name) {
        return jdbcTemplate.query(
            "select * from MOVIES where MOVIE_TITLE = ?",
            (rs, row) -> createMovie(rs),
            name
        );
    }

    private Movie createMovie(ResultSet rs) throws SQLException {
        long priceCategoryId = rs.getLong("PRICECATEGORY_FK");
        return Movie.of(
                rs.getString("MOVIE_TITLE"),
                rs.getDate("MOVIE_RELEASEDATE").toLocalDate(),
                rs.getBoolean("MOVIE_RENTED"),
                priceCategoryRepo.findById(priceCategoryId).get())
            .withId(rs.getLong("MOVIE_ID"));
    }

    @Override
    public Movie save(Movie movie) {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        final String INSERT_SQL;
        if (movie.getId() == null) {
            INSERT_SQL = "insert into movies (movie_releasedate, movie_title, movie_rented, pricecategory_fk) values (?, ?, ?, ?)";
        } else {
            INSERT_SQL = "update movies set movie_releasedate = ?, movie_title = ?, movie_rented = ?, pricecategory_fk = ? where MOVIE_ID = ?";
        }

        jdbcTemplate.update(
            connection -> {
                PreparedStatement ps =
                    connection.prepareStatement(INSERT_SQL, new String[] {"MOVIE_ID"});
                ps.setDate(1, java.sql.Date.valueOf(movie.getReleaseDate()));
                ps.setString(2, movie.getTitle());
                ps.setBoolean(3, movie.isRented());
                ps.setLong(4, movie.getPriceCategory().getId());

                if (movie.getId() != null) {
                    ps.setLong(5, movie.getId());
                }

                return ps;
            },
            keyHolder);

        if (movie.getId() == null) {
            return movie.withId(Objects.requireNonNull(keyHolder.getKey()).longValue());
        } else {
            return movie;
        }
    }

    @Override
    public void delete(Movie movie) {
        if (movie == null) throw new IllegalArgumentException();
        jdbcTemplate.update(
            "delete from MOVIES where MOVIE_ID = ?",
            movie.getId()
        );

        movie.setId(null);
    }

    @Override
    public void deleteById(Long id) {
        if (id == null) throw new IllegalArgumentException();
        findById(id).ifPresent(this::delete);
    }

    @Override
    public boolean existsById(Long id) {
        return findById(id).isPresent();
    }

    @Override
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM MOVIES", Long.class);
    }

}
