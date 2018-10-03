package ch.fhnw.edu.rental.persistence.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import ch.fhnw.edu.rental.model.Movie;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import ch.fhnw.edu.rental.model.Rental;
import ch.fhnw.edu.rental.model.User;
import ch.fhnw.edu.rental.persistence.RentalRepository;
import ch.fhnw.edu.rental.persistence.UserRepository;

@Component
public class UserRepositoryImpl implements UserRepository {

    private final JdbcTemplate jdbcTemplate;
	private final RentalRepository rentalRepo;

    public UserRepositoryImpl(JdbcTemplate jdbcTemplate, @Lazy RentalRepository rentalRepo) {
        this.jdbcTemplate = jdbcTemplate;
        this.rentalRepo = rentalRepo;
    }

    @Override
	public Optional<User> findById(Long id) {
        List<User> rental = jdbcTemplate.query(
            "select * from USERS where USER_ID = ?",
            (rs, row) -> createUser(rs),
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

    private User createUser(ResultSet rs) throws SQLException {

        long id = rs.getLong("USER_ID");
        String email = rs.getString("USER_EMAIL");
        String firstName = rs.getString("USER_FIRSTNAME");
        String userName = rs.getString("USER_NAME");

        User user = new User(userName, firstName);
        user.setEmail(email);
        user.setId(id);
        user.setRentals(rentalRepo.findByUser(user));

        return user;
    }

    @Override
	public List<User> findAll() {
        return jdbcTemplate.query(
            "select * from USERS",
            (rs, row) -> createUser(rs)
        );
	}

	@Override
	public User save(User user) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        String INSERT_SQL = "insert into users (user_name, user_firstname, user_email) values (?, ?, ?);";
        jdbcTemplate.update(
            connection -> {
                PreparedStatement ps =
                    connection.prepareStatement(INSERT_SQL, new String[] {"rental_id"});
                ps.setString(1, user.getLastName());
                ps.setString(2, user.getFirstName());
                ps.setString(3, user.getEmail());
                return ps;
            },
            keyHolder);

        user.setId(Objects.requireNonNull(keyHolder.getKey()).longValue());
		return user;
	}

	@Override
	public void delete(User user) {
		if(user == null) throw new IllegalArgumentException();
		for(Rental r : user.getRentals()){
			rentalRepo.delete(r);
		}
        jdbcTemplate.update(
            "delete from USERS where USER_ID = ?",
            user.getId()
        );
	}

	@Override
	public void deleteById(Long id) {
		if(id == null) throw new IllegalArgumentException();
		findById(id).ifPresent(this::delete);
	}

	@Override
	public boolean existsById(Long id) {
		if(id == null) throw new IllegalArgumentException();
        return findById(id).isPresent();
	}

	@Override
	public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM USERS", Long.class);
	}

	@Override
	public List<User> findByLastName(String lastName) {
        if (lastName == null || lastName.isEmpty()) {
            throw new IllegalArgumentException("No empty name");
        }
		// TODO use SQL
        return findAll().stream()
            .filter(user -> lastName.equals(user.getLastName())).collect(Collectors.toList());
	}

	@Override
	public List<User> findByFirstName(String firstName) {
        if (firstName == null || firstName.isEmpty()) {
            throw new IllegalArgumentException("No empty firstName");
        }
        // TODO use SQL
        return findAll().stream()
            .filter(user -> firstName.equals(user.getFirstName())).collect(Collectors.toList());
	}

	@Override
	public List<User> findByEmail(String email) {
        if (email == null || email.isEmpty()) {
            throw new IllegalArgumentException("No empty mail");
        }
        // TODO use SQL
        return findAll().stream()
            .filter(user -> email.equals(user.getEmail())).collect(Collectors.toList());
	}

}
