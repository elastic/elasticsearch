package graphql.schema.idl;

import graphql.PublicApi;
import graphql.language.InterfaceTypeDefinition;

@PublicApi
public class InterfaceWiringEnvironment extends WiringEnvironment {

    private final InterfaceTypeDefinition interfaceTypeDefinition;

    InterfaceWiringEnvironment(TypeDefinitionRegistry registry, InterfaceTypeDefinition interfaceTypeDefinition) {
        super(registry);
        this.interfaceTypeDefinition = interfaceTypeDefinition;
    }

    public InterfaceTypeDefinition getInterfaceTypeDefinition() {
        return interfaceTypeDefinition;
    }
}