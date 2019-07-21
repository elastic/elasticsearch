package graphql.schema.visibility;

import graphql.PublicApi;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLInputFieldsContainer;
import graphql.schema.GraphQLInputObjectField;

import java.util.List;

/**
 * This allows you to control the visibility of graphql fields.  By default
 * graphql-java makes every defined field visible but you can implement an instance of this
 * interface and reduce specific field visibility.
 */
@PublicApi
public interface GraphqlFieldVisibility {

    /**
     * Called to get the list of fields from an object type or interface
     *
     * @param fieldsContainer the type in play
     *
     * @return a non null list of {@link graphql.schema.GraphQLFieldDefinition}s
     */
    List<GraphQLFieldDefinition> getFieldDefinitions(GraphQLFieldsContainer fieldsContainer);

    /**
     * Called to get a named field from an object type or interface
     *
     * @param fieldsContainer the type in play
     * @param fieldName       the name of the desired field
     *
     * @return a {@link graphql.schema.GraphQLFieldDefinition} or null if its not visible
     */
    GraphQLFieldDefinition getFieldDefinition(GraphQLFieldsContainer fieldsContainer, String fieldName);


    /**
     * Called to get the list of fields from an input object type
     *
     * @param fieldsContainer the type in play
     *
     * @return a non null list of {@link graphql.schema.GraphQLInputObjectField}s
     */
    default List<GraphQLInputObjectField> getFieldDefinitions(GraphQLInputFieldsContainer fieldsContainer) {
        return fieldsContainer.getFieldDefinitions();
    }

    /**
     * Called to get a named field from an input object type
     *
     * @param fieldsContainer the type in play
     * @param fieldName       the name of the desired field
     *
     * @return a {@link graphql.schema.GraphQLInputObjectField} or null if its not visible
     */
    default GraphQLInputObjectField getFieldDefinition(GraphQLInputFieldsContainer fieldsContainer, String fieldName) {
        return fieldsContainer.getFieldDefinition(fieldName);
    }

}
