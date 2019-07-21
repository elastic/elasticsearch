package graphql.schema.idl;

import graphql.Scalars;
import graphql.language.ScalarTypeDefinition;
import graphql.schema.GraphQLScalarType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Info on all the standard scalar objects provided by graphql-java
 */
public class ScalarInfo {
    /**
     * A list of the scalar types provided by graphql-java
     */
    public static final List<GraphQLScalarType> STANDARD_SCALARS = new ArrayList<>();

    /**
     * A list of the built-in scalar types as defined by the graphql specification
     */
    public static final List<GraphQLScalarType> GRAPHQL_SPECIFICATION_SCALARS = new ArrayList<>();

    /**
     * A map of scalar type definitions provided by graphql-java
     */
    public static final Map<String, ScalarTypeDefinition> STANDARD_SCALAR_DEFINITIONS = new LinkedHashMap<>();

    static {
        GRAPHQL_SPECIFICATION_SCALARS.add(Scalars.GraphQLInt);
        GRAPHQL_SPECIFICATION_SCALARS.add(Scalars.GraphQLFloat);
        GRAPHQL_SPECIFICATION_SCALARS.add(Scalars.GraphQLString);
        GRAPHQL_SPECIFICATION_SCALARS.add(Scalars.GraphQLBoolean);
        GRAPHQL_SPECIFICATION_SCALARS.add(Scalars.GraphQLID);

        STANDARD_SCALARS.add(Scalars.GraphQLInt);
        STANDARD_SCALARS.add(Scalars.GraphQLFloat);
        STANDARD_SCALARS.add(Scalars.GraphQLString);
        STANDARD_SCALARS.add(Scalars.GraphQLBoolean);
        STANDARD_SCALARS.add(Scalars.GraphQLID);

        STANDARD_SCALARS.add(Scalars.GraphQLBigDecimal);
        STANDARD_SCALARS.add(Scalars.GraphQLBigInteger);
        STANDARD_SCALARS.add(Scalars.GraphQLByte);
        STANDARD_SCALARS.add(Scalars.GraphQLChar);
        STANDARD_SCALARS.add(Scalars.GraphQLShort);
        STANDARD_SCALARS.add(Scalars.GraphQLLong);
    }

    static {
        // graphql standard scalars
        STANDARD_SCALAR_DEFINITIONS.put("Int", ScalarTypeDefinition.newScalarTypeDefinition().name("Int").build());
        STANDARD_SCALAR_DEFINITIONS.put("Float", ScalarTypeDefinition.newScalarTypeDefinition().name("Float").build());
        STANDARD_SCALAR_DEFINITIONS.put("String", ScalarTypeDefinition.newScalarTypeDefinition().name("String").build());
        STANDARD_SCALAR_DEFINITIONS.put("Boolean", ScalarTypeDefinition.newScalarTypeDefinition().name("Boolean").build());
        STANDARD_SCALAR_DEFINITIONS.put("ID", ScalarTypeDefinition.newScalarTypeDefinition().name("ID").build());

        // graphql-java library extensions
        STANDARD_SCALAR_DEFINITIONS.put("Long", ScalarTypeDefinition.newScalarTypeDefinition().name("Long").build());
        STANDARD_SCALAR_DEFINITIONS.put("BigInteger", ScalarTypeDefinition.newScalarTypeDefinition().name("BigInteger").build());
        STANDARD_SCALAR_DEFINITIONS.put("BigDecimal", ScalarTypeDefinition.newScalarTypeDefinition().name("BigDecimal").build());
        STANDARD_SCALAR_DEFINITIONS.put("Short", ScalarTypeDefinition.newScalarTypeDefinition().name("Short").build());
        STANDARD_SCALAR_DEFINITIONS.put("Char", ScalarTypeDefinition.newScalarTypeDefinition().name("Char").build());

    }

    /**
     * Returns true if the scalar type is a standard one provided by graphql-java
     *
     * @param scalarType the type in question
     *
     * @return true if the scalar type is a graphql-java provided scalar
     */
    public static boolean isStandardScalar(GraphQLScalarType scalarType) {
        return inList(STANDARD_SCALARS, scalarType.getName());
    }

    /**
     * Returns true if the scalar type is a standard one provided by graphql-java
     *
     * @param scalarTypeName the name of the scalar type in question
     *
     * @return true if the scalar type is a graphql-java provided scalar
     */
    public static boolean isStandardScalar(String scalarTypeName) {
        return inList(STANDARD_SCALARS, scalarTypeName);
    }

    /**
     * Returns true if the scalar type is a scalar that is specified by the graphql specification
     *
     * @param scalarTypeName the name of the scalar type in question
     *
     * @return true if the scalar type is is specified by the graphql specification
     */
    public static boolean isGraphqlSpecifiedScalar(String scalarTypeName) {
        return inList(GRAPHQL_SPECIFICATION_SCALARS, scalarTypeName);
    }

    /**
     * Returns true if the scalar type is a scalar that is specified by the graphql specification
     *
     * @param scalarType the type in question
     *
     * @return true if the scalar type is is specified by the graphql specification
     */
    public static boolean isGraphqlSpecifiedScalar(GraphQLScalarType scalarType) {
        return inList(GRAPHQL_SPECIFICATION_SCALARS, scalarType.getName());
    }

    private static boolean inList(List<GraphQLScalarType> scalarList, String scalarTypeName) {
        return scalarList.stream().anyMatch(sc -> sc.getName().equals(scalarTypeName));
    }

}
