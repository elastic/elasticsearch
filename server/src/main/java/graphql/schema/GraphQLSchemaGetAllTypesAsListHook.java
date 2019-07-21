package graphql.schema;

import java.util.List;

public interface GraphQLSchemaGetAllTypesAsListHook {
    public List<GraphQLType> getAllTypesAsList(List<GraphQLType> list);
}
