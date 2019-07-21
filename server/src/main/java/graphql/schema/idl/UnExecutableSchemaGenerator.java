package graphql.schema.idl;

import graphql.Internal;
import graphql.language.ScalarTypeDefinition;
import graphql.schema.GraphQLSchema;

import java.util.Map;

import static graphql.schema.idl.EchoingWiringFactory.fakeScalar;

@Internal
public class UnExecutableSchemaGenerator {

    /*
     * Creates just enough runtime wiring to allow a schema to be built but which CANT
     * be sensibly executed
     */
    public static GraphQLSchema makeUnExecutableSchema(TypeDefinitionRegistry registry) {
        RuntimeWiring runtimeWiring = EchoingWiringFactory.newEchoingWiring(wiring -> {
            Map<String, ScalarTypeDefinition> scalars = registry.scalars();
            scalars.forEach((name, v) -> {
                if (!ScalarInfo.isStandardScalar(name)) {
                    wiring.scalar(fakeScalar(name));
                }
            });
        });

        return new SchemaGenerator().makeExecutableSchema(registry, runtimeWiring);
    }
}
