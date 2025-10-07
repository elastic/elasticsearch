package org.elasticsearch.index.query;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to validate field names for future shorthand support.
 */
public final class FieldNameValidator {

    private FieldNameValidator() {}

    /**
     * Ensures that the given field name is either fully qualified or
     * if it is a shorthand name, it uniquely resolves to a single field.
     *
     * @param fieldName the field name provided in the query DSL
     * @param context the SearchExecutionContext (used to inspect all mapped fields)
     */
    public static void ensureShorthandSafe(String fieldName, SearchExecutionContext context) {
        if (fieldName.contains(".")) {
            return;
        }

        List<String> matches = new ArrayList<>();

        for (Mapper mapper : context.getMappingLookup().fieldMappers()) {
            String fullName = mapper.fullPath();
            if (fullName.equals(fieldName) || fullName.endsWith("." + fieldName)) {
                matches.add(fullName);
            }
        }

        if (matches.size() > 1) {
            throw new IllegalArgumentException(
                "Ambiguous shorthand field name '" + fieldName + "'. " +
                    "It matches multiple fields: " + matches + ". " +
                    "Please use a fully qualified name like 'object." + fieldName + "'."
            );
        }
    }

    public static void ensureSyntaxSafe(String fieldName, XContentParser parser) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "Field name must not be empty");
        }
    }
}
