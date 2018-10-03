package ch.fhnw.edu.rental.persistence.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import ch.fhnw.edu.rental.model.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import ch.fhnw.edu.rental.persistence.PriceCategoryRepository;

@Component
public class PriceCategoryRepositoryImpl implements PriceCategoryRepository {

    private final JdbcTemplate jdbcTemplate;

    public PriceCategoryRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
	public Optional<PriceCategory> findById(Long id) {
        List<PriceCategory> priceCategories = jdbcTemplate.query(
            "select * from PRICECATEGORIES where PRICECATEGORY_ID = ?",
            (rs, row) -> createPriceCategory(rs),
            id
        );

        if (priceCategories.size() == 0) {
            return Optional.empty();
        } else if (priceCategories.size() > 1) {
            throw new IllegalStateException("Multiple priceCategories have the same id");
        } else {
            return Optional.of(priceCategories.get(0));
        }
	}

    private PriceCategory createPriceCategory(ResultSet rs) throws SQLException {
        long id = rs.getLong("PRICECATEGORY_ID");
        String type = rs.getString("PRICECATEGORY_TYPE");

        PriceCategory category;
        switch (type) {
            case "Regular": category = new PriceCategoryRegular(); break;
            case "Children": category = new PriceCategoryChildren(); break;
            case "NewRelease": category = new PriceCategoryNewRelease(); break;
            default:
                throw new IllegalArgumentException("Unkown price cateogry");
        }

        category.setId(id);

        return category;
    }

    @Override
	public List<PriceCategory> findAll() {
        return jdbcTemplate.query(
            "select * from pricecategories",
            (rs, row) -> createPriceCategory(rs)
        );
	}

	@Override
	public PriceCategory save(PriceCategory category) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        String INSERT_SQL = "insert into pricecategories (pricecategory_id, pricecategory_type) values (?, ?);";
        jdbcTemplate.update(
            connection -> {
                PreparedStatement ps =
                    connection.prepareStatement(INSERT_SQL, new String[] {"pricecategory_id"});
                ps.setLong(1, category.getId());
                ps.setString(2, category.toString());
                return ps;
            },
            keyHolder);

        // todo fix
        return category;
//        return movie.withId(Objects.requireNonNull(keyHolder.getKey()).longValue());
	}

	@Override
	public void delete(PriceCategory priceCategory) {
		if(priceCategory == null) throw new IllegalArgumentException();
        jdbcTemplate.update(
            "delete from pricecategories where pricecategory_id = ?",
            priceCategory.getId()
        );
	}

	@Override
	public void deleteById(Long id) {
		if(id == null) throw new IllegalArgumentException();
		findById(id).ifPresent(this::delete);
	}

	@Override
	public boolean existsById(Long id) {
        return findById(id).isPresent();
	}

	@Override
	public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM pricecategories", Long.class);
	}

}
