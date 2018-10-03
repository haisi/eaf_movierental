package ch.fhnw.edu.rental.persistence.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import ch.fhnw.edu.rental.model.Movie;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import ch.fhnw.edu.rental.model.Rental;
import ch.fhnw.edu.rental.model.User;
import ch.fhnw.edu.rental.persistence.MovieRepository;
import ch.fhnw.edu.rental.persistence.RentalRepository;
import ch.fhnw.edu.rental.persistence.UserRepository;

@Component
public class RentalRepositoryImpl implements RentalRepository {

    private final JdbcTemplate jdbcTemplate;
	private final UserRepository userRepo;
	private final MovieRepository movieRepo;

    public RentalRepositoryImpl(JdbcTemplate jdbcTemplate, UserRepository userRepo, MovieRepository movieRepo) {
        this.jdbcTemplate = jdbcTemplate;
        this.userRepo = userRepo;
        this.movieRepo = movieRepo;
    }

	@Override
	public Optional<Rental> findById(Long id) {
		if(id == null) throw new IllegalArgumentException();
        List<Rental> rental = jdbcTemplate.query(
            "select * from RENTALS where RENTAL_ID = ?",
            (rs, row) -> createRental(rs),
            id
        );

        if (rental.size() == 0) {
            return Optional.empty();
        } else if (rental.size() > 1) {
            throw new IllegalStateException("Multiple rental have the same id");
        } else {
            return Optional.of(rental.get(0));
        }
	}

    private Rental createRental(ResultSet rs) throws SQLException {

        long userId = rs.getLong("USER_ID");
        User user = userRepo.findById(userId).orElseThrow(() -> new IllegalArgumentException("User not found"));

        long movieId = rs.getLong("MOVIE_ID");
        Movie movie = movieRepo.findById(movieId).orElseThrow(() -> new IllegalArgumentException("Movie not found"));

        long rentalId = rs.getLong("RENTAL_ID");

        int rentalDays = rs.getInt("RENTAL_RENTALDAYS");
        Rental rental = new Rental(user, movie, rentalDays);
        rental.setId(rentalId);

        return rental;
    }

    @Override
	public List<Rental> findAll() {
        return jdbcTemplate.query(
            "select * from RENTALS",
            (rs, row) -> createRental(rs)
        );
	}

	@Override
	public List<Rental> findByUser(User user) {
        return jdbcTemplate.query(
            "select * from RENTALS where USER_ID = ?",
            (rs, row) -> createRental(rs),
            user.getId()
        );
	}

	@Override
	public Rental save(Rental rental) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        String INSERT_SQL = "insert into rentals (movie_id, user_id, rental_rentaldate, rental_rentaldays) values (?, ?, ?, ?);";
        jdbcTemplate.update(
            connection -> {
                PreparedStatement ps =
                    connection.prepareStatement(INSERT_SQL, new String[] {"rental_id"});
                ps.setLong(1, rental.getUser().getId());
                ps.setLong(2, rental.getMovie().getId());
                ps.setDate(3, java.sql.Date.valueOf(rental.getRentalDate()));
                ps.setInt(4, rental.getRentalDays());
                return ps;
            },
            keyHolder);

        rental.setId(Objects.requireNonNull(keyHolder.getKey()).longValue());
        return rental;
	}

	@Override
	public void delete(Rental rental) {
		if(rental == null) throw new IllegalArgumentException();
		deleteById(rental.getId());
	}

	@Override
	public void deleteById(Long id) {
		if(id == null) throw new IllegalArgumentException();
        jdbcTemplate.update(
            "delete from RENTALS where RENTAL_ID = ?",
            id
        );
	}

	@Override
	public boolean existsById(Long id) {
		if(id == null) throw new IllegalArgumentException();
		return findById(id).isPresent();
	}

	@Override
	public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM RENTALS", Long.class);
	}

}
