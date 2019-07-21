package graphql.introspection;

public interface IntrospectionQuery {

    String INTROSPECTION_QUERY = "\n" +
            "  query IntrospectionQuery {\n" +
            "    __schema {\n" +
            "      queryType { name }\n" +
            "      mutationType { name }\n" +
            "      subscriptionType { name }\n" +
            "      types {\n" +
            "        ...FullType\n" +
            "      }\n" +
            "      directives {\n" +
            "        name\n" +
            "        description\n" +
            "        locations\n" +
            "        args {\n" +
            "          ...InputValue\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "\n" +
            "  fragment FullType on __Type {\n" +
            "    kind\n" +
            "    name\n" +
            "    description\n" +
            "    fields(includeDeprecated: true) {\n" +
            "      name\n" +
            "      description\n" +
            "      args {\n" +
            "        ...InputValue\n" +
            "      }\n" +
            "      type {\n" +
            "        ...TypeRef\n" +
            "      }\n" +
            "      isDeprecated\n" +
            "      deprecationReason\n" +
            "    }\n" +
            "    inputFields {\n" +
            "      ...InputValue\n" +
            "    }\n" +
            "    interfaces {\n" +
            "      ...TypeRef\n" +
            "    }\n" +
            "    enumValues(includeDeprecated: true) {\n" +
            "      name\n" +
            "      description\n" +
            "      isDeprecated\n" +
            "      deprecationReason\n" +
            "    }\n" +
            "    possibleTypes {\n" +
            "      ...TypeRef\n" +
            "    }\n" +
            "  }\n" +
            "\n" +
            "  fragment InputValue on __InputValue {\n" +
            "    name\n" +
            "    description\n" +
            "    type { ...TypeRef }\n" +
            "    defaultValue\n" +
            "  }\n" +
            "\n" +
            //
            // The depth of the types is actually an arbitrary decision.  It could be any depth in fact.  This depth
            // was taken from GraphIQL https://github.com/graphql/graphiql/blob/master/src/utility/introspectionQueries.js
            // which uses 7 levels and hence could represent a type like say [[[[[Float!]]]]]
            //
            "fragment TypeRef on __Type {\n" +
            "    kind\n" +
            "    name\n" +
            "    ofType {\n" +
            "      kind\n" +
            "      name\n" +
            "      ofType {\n" +
            "        kind\n" +
            "        name\n" +
            "        ofType {\n" +
            "          kind\n" +
            "          name\n" +
            "          ofType {\n" +
            "            kind\n" +
            "            name\n" +
            "            ofType {\n" +
            "              kind\n" +
            "              name\n" +
            "              ofType {\n" +
            "                kind\n" +
            "                name\n" +
            "                ofType {\n" +
            "                  kind\n" +
            "                  name\n" +
            "                }\n" +
            "              }\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "\n";
}
