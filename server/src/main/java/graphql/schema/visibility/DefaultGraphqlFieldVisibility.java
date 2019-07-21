package graphql.schema.visibility;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLInputFieldsContainer;
import graphql.schema.GraphQLInputObjectField;

import java.util.List;

/**
 * The default field visibility of graphql-java is that everything is visible
 */
public class DefaultGraphqlFieldVisibility implements GraphqlFieldVisibility {

    public static final DefaultGraphqlFieldVisibility DEFAULT_FIELD_VISIBILITY = new DefaultGraphqlFieldVisibility();

    @Override
    public List<GraphQLFieldDefinition> getFieldDefinitions(GraphQLFieldsContainer fieldsContainer) {
        return fieldsContainer.getFieldDefinitions();
    }

    @Override
    public GraphQLFieldDefinition getFieldDefinition(GraphQLFieldsContainer fieldsContainer, String fieldName) {
        return fieldsContainer.getFieldDefinition(fieldName);
    }

    @Override
    public List<GraphQLInputObjectField> getFieldDefinitions(GraphQLInputFieldsContainer fieldsContainer) {
        return fieldsContainer.getFieldDefinitions();
    }

    @Override
    public GraphQLInputObjectField getFieldDefinition(GraphQLInputFieldsContainer fieldsContainer, String fieldName) {
        return fieldsContainer.getFieldDefinition(fieldName);
    }
}