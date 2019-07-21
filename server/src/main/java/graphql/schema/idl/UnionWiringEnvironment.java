package graphql.schema.idl;

import graphql.PublicApi;
import graphql.language.UnionTypeDefinition;

@PublicApi
public class UnionWiringEnvironment extends WiringEnvironment {

    private final UnionTypeDefinition unionTypeDefinition;

    UnionWiringEnvironment(TypeDefinitionRegistry registry, UnionTypeDefinition unionTypeDefinition) {
        super(registry);
        this.unionTypeDefinition = unionTypeDefinition;
    }

    public UnionTypeDefinition getUnionTypeDefinition() {
        return unionTypeDefinition;
    }
}