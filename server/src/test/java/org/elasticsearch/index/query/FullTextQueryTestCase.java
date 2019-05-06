package org.elasticsearch.index.query;

import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class FullTextQueryTestCase<QB extends AbstractQueryBuilder<QB>> extends AbstractQueryTestCase<QB> {
    protected abstract boolean isCacheable(QB queryBuilder);

    /**
     * Full text queries that start with "now" are not cacheable if they
     * target a {@link DateFieldMapper.DateFieldType} field.
     */
    protected final boolean isCacheable(Collection<String> fields, String value) {
        if (value.length() < 3
                || value.substring(0, 3).equalsIgnoreCase("now") == false) {
            return true;
        }
        Set<String> dateFields = new HashSet<>();
        getMapping().forEach(ft -> {
            if (ft instanceof DateFieldMapper.DateFieldType) {
                dateFields.add(ft.name());
            }
        });
        for (MappedFieldType ft : getMapping()) {
            if (ft instanceof DateFieldMapper.DateFieldType) {
                dateFields.add(ft.name());
            }
        }
        if (fields.isEmpty()) {
            // special case: all fields are requested
            return dateFields.isEmpty();
        }
        return fields.stream()
            .anyMatch(dateFields::contains) == false;
    }
}
