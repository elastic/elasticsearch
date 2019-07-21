package graphql.schema.visibility;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;

import java.util.List;

import static graphql.schema.visibility.BlockedFields.newBlock;

/**
 * This field visibility will prevent Introspection queries from being performed.  Technically this puts your
 * system in contravention of the specification - http://facebook.github.io/graphql/#sec-Introspection but some
 * production systems want this lock down in place.
 */
public class NoIntrospectionGraphqlFieldVisibility implements GraphqlFieldVisibility {

    public static NoIntrospectionGraphqlFieldVisibility NO_INTROSPECTION_FIELD_VISIBILITY = new NoIntrospectionGraphqlFieldVisibility();


    private final BlockedFields blockedFields;

    public NoIntrospectionGraphqlFieldVisibility() {
        blockedFields = newBlock().addPattern("__.*").build();
    }

    @Override
    public List<GraphQLFieldDefinition> getFieldDefinitions(GraphQLFieldsContainer fieldsContainer) {
        return blockedFields.getFieldDefinitions(fieldsContainer);
    }

    @Override
    public GraphQLFieldDefinition getFieldDefinition(GraphQLFieldsContainer fieldsContainer, String fieldName) {
        return blockedFields.getFieldDefinition(fieldsContainer, fieldName);
    }

}
