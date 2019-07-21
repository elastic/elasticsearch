package graphql.analysis;

import graphql.Internal;
import graphql.language.Field;
import graphql.language.Node;
import graphql.language.SelectionSetContainer;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLOutputType;
import graphql.util.TraverserContext;

import java.util.Map;
import java.util.Objects;

@Internal
public class QueryVisitorFieldEnvironmentImpl implements QueryVisitorFieldEnvironment {

    private final boolean typeNameIntrospectionField;
    private final Field field;
    private final GraphQLFieldDefinition fieldDefinition;
    private final GraphQLOutputType parentType;
    private final GraphQLFieldsContainer unmodifiedParentType;
    private final Map<String, Object> arguments;
    private final QueryVisitorFieldEnvironment parentEnvironment;
    private final SelectionSetContainer selectionSetContainer;
    private final TraverserContext<Node> traverserContext;

    public QueryVisitorFieldEnvironmentImpl(boolean typeNameIntrospectionField,
                                            Field field,
                                            GraphQLFieldDefinition fieldDefinition,
                                            GraphQLOutputType parentType,
                                            GraphQLFieldsContainer unmodifiedParentType,
                                            QueryVisitorFieldEnvironment parentEnvironment,
                                            Map<String, Object> arguments,
                                            SelectionSetContainer selectionSetContainer,
                                            TraverserContext<Node> traverserContext) {
        this.typeNameIntrospectionField = typeNameIntrospectionField;
        this.field = field;
        this.fieldDefinition = fieldDefinition;
        this.parentType = parentType;
        this.unmodifiedParentType = unmodifiedParentType;
        this.parentEnvironment = parentEnvironment;
        this.arguments = arguments;
        this.selectionSetContainer = selectionSetContainer;
        this.traverserContext = traverserContext;
    }

    @Override
    public Field getField() {
        return field;
    }

    @Override
    public GraphQLFieldDefinition getFieldDefinition() {
        return fieldDefinition;
    }

    @Override
    public GraphQLOutputType getParentType() {
        return parentType;
    }

    @Override
    public QueryVisitorFieldEnvironment getParentEnvironment() {
        return parentEnvironment;
    }

    @Override
    public Map<String, Object> getArguments() {
        return arguments;
    }

    @Override
    public SelectionSetContainer getSelectionSetContainer() {
        return selectionSetContainer;
    }

    @Override
    public GraphQLFieldsContainer getFieldsContainer() {
        if (isTypeNameIntrospectionField()) {
            throw new IllegalStateException("introspection field __typename doesn't have a fields container");
        }
        return unmodifiedParentType;
    }

    @Override
    public boolean isTypeNameIntrospectionField() {
        return typeNameIntrospectionField;
    }

    @Override
    public TraverserContext<Node> getTraverserContext() {
        return traverserContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryVisitorFieldEnvironmentImpl that = (QueryVisitorFieldEnvironmentImpl) o;
        return typeNameIntrospectionField == that.typeNameIntrospectionField &&
                Objects.equals(field, that.field) &&
                Objects.equals(fieldDefinition, that.fieldDefinition) &&
                Objects.equals(parentType, that.parentType) &&
                Objects.equals(unmodifiedParentType, that.unmodifiedParentType) &&
                Objects.equals(arguments, that.arguments) &&
                Objects.equals(parentEnvironment, that.parentEnvironment) &&
                Objects.equals(selectionSetContainer, that.selectionSetContainer);
    }

    @Override
    public int hashCode() {

        return Objects.hash(typeNameIntrospectionField, field, fieldDefinition, parentType, unmodifiedParentType, arguments, parentEnvironment, selectionSetContainer);
    }

    @Override
    public String toString() {
        return "QueryVisitorFieldEnvironmentImpl{" +
                "field=" + field +
                ", fieldDefinition=" + fieldDefinition +
                ", parentType=" + parentType +
                ", parentEnvironment=" + parentEnvironment +
                ", arguments=" + arguments +
                '}';
    }
}
