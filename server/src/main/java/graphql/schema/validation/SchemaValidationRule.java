package graphql.schema.validation;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLType;

public interface SchemaValidationRule {

    void check(GraphQLFieldDefinition fieldDef, SchemaValidationErrorCollector validationErrorCollector);

    void check(GraphQLType type, SchemaValidationErrorCollector validationErrorCollector);
}
