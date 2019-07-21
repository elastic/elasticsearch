package graphql.analysis;

import graphql.PublicApi;
import graphql.language.Field;
import graphql.schema.GraphQLCompositeType;
import graphql.schema.GraphQLFieldDefinition;

import java.util.Map;

@PublicApi
public class FieldComplexityEnvironment {
    private final Field field;
    private final GraphQLFieldDefinition fieldDefinition;
    private final GraphQLCompositeType parentType;
    private final FieldComplexityEnvironment parentEnvironment;
    private final Map<String, Object> arguments;

    public FieldComplexityEnvironment(Field field, GraphQLFieldDefinition fieldDefinition, GraphQLCompositeType parentType, Map<String, Object> arguments, FieldComplexityEnvironment parentEnvironment) {
        this.field = field;
        this.fieldDefinition = fieldDefinition;
        this.parentType = parentType;
        this.arguments = arguments;
        this.parentEnvironment = parentEnvironment;
    }

    public Field getField() {
        return field;
    }

    public GraphQLFieldDefinition getFieldDefinition() {
        return fieldDefinition;
    }

    public GraphQLCompositeType getParentType() {
        return parentType;
    }

    public FieldComplexityEnvironment getParentEnvironment() {
        return parentEnvironment;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return "FieldComplexityEnvironment{" +
                "field=" + field +
                ", fieldDefinition=" + fieldDefinition +
                ", parentType=" + parentType +
                ", parentEnvironment=" + parentEnvironment +
                ", arguments=" + arguments +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldComplexityEnvironment that = (FieldComplexityEnvironment) o;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (fieldDefinition != null ? !fieldDefinition.equals(that.fieldDefinition) : that.fieldDefinition != null)
            return false;
        if (parentType != null ? !parentType.equals(that.parentType) : that.parentType != null) return false;
        if (parentEnvironment != null ? !parentEnvironment.equals(that.parentEnvironment) : that.parentEnvironment != null)
            return false;
        return arguments != null ? arguments.equals(that.arguments) : that.arguments == null;
    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (fieldDefinition != null ? fieldDefinition.hashCode() : 0);
        result = 31 * result + (parentType != null ? parentType.hashCode() : 0);
        result = 31 * result + (parentEnvironment != null ? parentEnvironment.hashCode() : 0);
        result = 31 * result + (arguments != null ? arguments.hashCode() : 0);
        return result;
    }
}


