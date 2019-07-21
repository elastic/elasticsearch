package graphql.schema;


import graphql.Internal;
import graphql.TypeResolutionEnvironment;

@Internal
public class TypeResolverProxy implements TypeResolver {

    private TypeResolver typeResolver;

    public TypeResolver getTypeResolver() {
        return typeResolver;
    }

    public void setTypeResolver(TypeResolver typeResolver) {
        this.typeResolver = typeResolver;
    }

    @Override
    public GraphQLObjectType getType(TypeResolutionEnvironment env) {
        return typeResolver != null ? typeResolver.getType(env) : null;
    }
}
