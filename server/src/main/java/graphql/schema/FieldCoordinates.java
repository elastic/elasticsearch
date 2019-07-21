package graphql.schema;

import graphql.PublicApi;

import java.util.Objects;

import static graphql.Assert.assertTrue;
import static graphql.Assert.assertValidName;

/**
 * A field in graphql is uniquely located within a parent type and hence code elements
 * like {@link graphql.schema.DataFetcher} need to be specified using those coordinates.
 */
@PublicApi
public class FieldCoordinates {

    private final String typeName;
    private final String fieldName;

    private FieldCoordinates(String typeName, String fieldName) {
        this.typeName = typeName;
        this.fieldName = fieldName;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldCoordinates that = (FieldCoordinates) o;
        return Objects.equals(typeName, that.typeName) &&
                Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, fieldName);
    }

    @Override
    public String toString() {
        return typeName + ':' + fieldName + '\'';
    }

    /**
     * Creates new field coordinates
     *
     * @param parentType      the container of the field
     * @param fieldDefinition the field definition
     *
     * @return new field coordinates represented by the two parameters
     */
    public static FieldCoordinates coordinates(GraphQLFieldsContainer parentType, GraphQLFieldDefinition fieldDefinition) {
        return new FieldCoordinates(parentType.getName(), fieldDefinition.getName());
    }

    /**
     * Creates new field coordinates
     *
     * @param parentType the container of the field
     * @param fieldName  the field name
     *
     * @return new field coordinates represented by the two parameters
     */
    public static FieldCoordinates coordinates(String parentType, String fieldName) {
        assertValidName(parentType);
        assertValidName(fieldName);
        return new FieldCoordinates(parentType, fieldName);
    }

    /**
     * The exception to the general rule is the system __xxxx Introspection fields which have no parent type and
     * are able to be specified on any type
     *
     * @param fieldName the name of the system field which MUST start with __
     *
     * @return the coordinates
     */
    public static FieldCoordinates systemCoordinates(String fieldName) {
        assertTrue(fieldName.startsWith("__"), "Only __ system fields can be addressed without a parent type");
        assertValidName(fieldName);
        return new FieldCoordinates(null, fieldName);
    }
}
