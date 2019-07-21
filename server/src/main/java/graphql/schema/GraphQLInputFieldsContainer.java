package graphql.schema;

import graphql.PublicApi;

import java.util.List;

/**
 * Types that can contain input fields are marked with this interface
 *
 * @see graphql.schema.GraphQLInputType
 */
@PublicApi
public interface GraphQLInputFieldsContainer extends GraphQLType {

    GraphQLInputObjectField getFieldDefinition(String name);

    List<GraphQLInputObjectField> getFieldDefinitions();
}