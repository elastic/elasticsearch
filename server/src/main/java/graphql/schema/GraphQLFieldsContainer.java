package graphql.schema;

import graphql.PublicApi;

import java.util.List;


/**
 * Types that can contain output fields are marked with this interface
 *
 * @see graphql.schema.GraphQLObjectType
 * @see graphql.schema.GraphQLInterfaceType
 */
@PublicApi
public interface GraphQLFieldsContainer extends GraphQLCompositeType {

    GraphQLFieldDefinition getFieldDefinition(String name);

    List<GraphQLFieldDefinition> getFieldDefinitions();
}
