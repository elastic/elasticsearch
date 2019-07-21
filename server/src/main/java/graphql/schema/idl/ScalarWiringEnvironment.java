package graphql.schema.idl;

import graphql.PublicApi;
import graphql.language.ScalarTypeDefinition;
import graphql.language.ScalarTypeExtensionDefinition;

import java.util.List;

@PublicApi
public class ScalarWiringEnvironment extends WiringEnvironment {

    private final ScalarTypeDefinition scalarTypeDefinition;
    private final List<ScalarTypeExtensionDefinition> extensions;

    ScalarWiringEnvironment(TypeDefinitionRegistry registry, ScalarTypeDefinition interfaceTypeDefinition, List<ScalarTypeExtensionDefinition> extensions) {
        super(registry);
        this.scalarTypeDefinition = interfaceTypeDefinition;
        this.extensions = extensions;
    }

    public ScalarTypeDefinition getScalarTypeDefinition() {
        return scalarTypeDefinition;
    }

    public List<ScalarTypeExtensionDefinition> getExtensions() {
        return extensions;
    }
}
