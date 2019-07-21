package graphql.relay;

import graphql.PublicApi;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.TypeResolver;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLID;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLInterfaceType.newInterface;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLTypeReference.typeRef;

/**
 * This can be used to compose graphql runtime types that implement
 * that Relay specification.
 *
 * See <a href="https://facebook.github.io/relay/graphql/connections.htm">https://facebook.github.io/relay/graphql/connections.htm</a>
 */
@PublicApi
public class Relay {

    public static final String NODE = "Node";

    public static final GraphQLObjectType pageInfoType = newObject()
            .name("PageInfo")
            .description("Information about pagination in a connection.")
            .field(newFieldDefinition()
                    .name("hasNextPage")
                    .type(nonNull(GraphQLBoolean))
                    .description("When paginating forwards, are there more items?"))
            .field(newFieldDefinition()
                    .name("hasPreviousPage")
                    .type(nonNull(GraphQLBoolean))
                    .description("When paginating backwards, are there more items?"))
            .field(newFieldDefinition()
                    .name("startCursor")
                    .type(GraphQLString)
                    .description("When paginating backwards, the cursor to continue."))
            .field(newFieldDefinition()
                    .name("endCursor")
                    .type(GraphQLString)
                    .description("When paginating forwards, the cursor to continue."))
            .build();

    public GraphQLInterfaceType nodeInterface(TypeResolver typeResolver) {
        return newInterface()
                .name(NODE)
                .description("An object with an ID")
                .typeResolver(typeResolver)
                .field(newFieldDefinition()
                        .name("id")
                        .description("The ID of an object")
                        .type(nonNull(GraphQLID)))
                .build();
    }

    public GraphQLFieldDefinition nodeField(GraphQLInterfaceType nodeInterface, DataFetcher nodeDataFetcher) {
        return newFieldDefinition()
                .name("node")
                .description("Fetches an object given its ID")
                .type(nodeInterface)
                .dataFetcher(nodeDataFetcher)
                .argument(newArgument()
                        .name("id")
                        .description("The ID of an object")
                        .type(nonNull(GraphQLID)))
                .build();
    }

    public List<GraphQLArgument> getConnectionFieldArguments() {
        List<GraphQLArgument> args = new ArrayList<>();
        args.add(newArgument()
                .name("before")
                .description("fetching only nodes before this node (exclusive)")
                .type(GraphQLString)
                .build());
        args.add(newArgument()
                .name("after")
                .description("fetching only nodes after this node (exclusive)")
                .type(GraphQLString)
                .build());
        args.add(newArgument()
                .name("first")
                .description("fetching only the first certain number of nodes")
                .type(GraphQLInt)
                .build());
        args.add(newArgument()
                .name("last")
                .description("fetching only the last certain number of nodes")
                .type(GraphQLInt)
                .build());
        return args;
    }

    public List<GraphQLArgument> getBackwardPaginationConnectionFieldArguments() {
        List<GraphQLArgument> args = new ArrayList<>();
        args.add(newArgument()
                .name("before")
                .description("fetching only nodes before this node (exclusive)")
                .type(GraphQLString)
                .build());
        args.add(newArgument()
                .name("last")
                .description("fetching only the last certain number of nodes")
                .type(GraphQLInt)
                .build());
        return args;
    }

    public List<GraphQLArgument> getForwardPaginationConnectionFieldArguments() {
        List<GraphQLArgument> args = new ArrayList<>();
        args.add(newArgument()
                .name("after")
                .description("fetching only nodes after this node (exclusive)")
                .type(GraphQLString)
                .build());
        args.add(newArgument()
                .name("first")
                .description("fetching only the first certain number of nodes")
                .type(GraphQLInt)
                .build());
        return args;
    }

    public GraphQLObjectType edgeType(String name, GraphQLOutputType nodeType, GraphQLInterfaceType nodeInterface, List<GraphQLFieldDefinition> edgeFields) {
        return newObject()
                .name(name + "Edge")
                .description("An edge in a connection")
                .field(newFieldDefinition()
                        .name("node")
                        .type(nodeType)
                        .description("The item at the end of the edge"))
                .field(newFieldDefinition()
                        .name("cursor")
                        .type(nonNull(GraphQLString))
                        .description("cursor marks a unique position or index into the connection"))
                .fields(edgeFields)
                .build();
    }

    public GraphQLObjectType connectionType(String name, GraphQLObjectType edgeType, List<GraphQLFieldDefinition> connectionFields) {
        return newObject()
                .name(name + "Connection")
                .description("A connection to a list of items.")
                .field(newFieldDefinition()
                        .name("edges")
                        .description("a list of edges")
                        .type(list(edgeType)))
                .field(newFieldDefinition()
                        .name("pageInfo")
                        .description("details about this specific page")
                        .type(nonNull(typeRef("PageInfo"))))
                .fields(connectionFields)
                .build();
    }

    public GraphQLFieldDefinition mutationWithClientMutationId(String name, String fieldName,
                                                               List<GraphQLInputObjectField> inputFields,
                                                               List<GraphQLFieldDefinition> outputFields,
                                                               DataFetcher dataFetcher) {
        GraphQLInputObjectField clientMutationIdInputField = newInputObjectField()
                .name("clientMutationId")
                .type(GraphQLString)
                .build();
        GraphQLFieldDefinition clientMutationIdPayloadField = newFieldDefinition()
                .name("clientMutationId")
                .type(GraphQLString)
                .build();

        return mutation(name, fieldName, addElementToList(inputFields, clientMutationIdInputField),
                addElementToList(outputFields, clientMutationIdPayloadField), dataFetcher);
    }

    private static <T> List<T> addElementToList(List<T> list, T element) {
        ArrayList<T> result = new ArrayList<>(list);
        result.add(element);
        return result;
    }

    public GraphQLFieldDefinition mutation(String name, String fieldName,
                                           List<GraphQLInputObjectField> inputFields,
                                           List<GraphQLFieldDefinition> outputFields,
                                           DataFetcher dataFetcher) {
        GraphQLInputObjectType inputObjectType = newInputObject()
                .name(name + "Input")
                .fields(inputFields)
                .build();
        GraphQLObjectType outputType = newObject()
                .name(name + "Payload")
                .fields(outputFields)
                .build();

        return newFieldDefinition()
                .name(fieldName)
                .type(outputType)
                .argument(newArgument()
                        .name("input")
                        .type(nonNull(inputObjectType)))
                .dataFetcher(dataFetcher)
                .build();
    }

    public static class ResolvedGlobalId {

        public ResolvedGlobalId(String type, String id) {
            this.type = type;
            this.id = id;
        }

        private final String type;
        private final String id;

        public String getType() {
            return type;
        }

        public String getId() {
            return id;
        }
    }

    private static final java.util.Base64.Encoder encoder = java.util.Base64.getUrlEncoder().withoutPadding();
    private static final java.util.Base64.Decoder decoder = java.util.Base64.getUrlDecoder();

    public String toGlobalId(String type, String id) {
        return encoder.encodeToString((type + ":" + id).getBytes(StandardCharsets.UTF_8));
    }

    public ResolvedGlobalId fromGlobalId(String globalId) {
        String[] split = new String(decoder.decode(globalId), StandardCharsets.UTF_8).split(":", 2);
        if (split.length != 2) {
            throw new IllegalArgumentException(String.format("expecting a valid global id, got %s", globalId));
        }
        return new ResolvedGlobalId(split[0], split[1]);
    }
}
