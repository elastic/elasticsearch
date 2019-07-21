package graphql.execution;


import graphql.Internal;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;

import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;

@Internal
public class TypeFromAST {


    public static GraphQLType getTypeFromAST(GraphQLSchema schema, Type type) {
        GraphQLType innerType;
        if (type instanceof ListType) {
            innerType = getTypeFromAST(schema, ((ListType) type).getType());
            return innerType != null ? list(innerType) : null;
        } else if (type instanceof NonNullType) {
            innerType = getTypeFromAST(schema, ((NonNullType) type).getType());
            return innerType != null ? nonNull(innerType) : null;
        }

        return schema.getType(((TypeName) type).getName());
    }
}
