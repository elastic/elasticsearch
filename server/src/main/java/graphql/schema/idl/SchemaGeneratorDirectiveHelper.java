package graphql.schema.idl;

import graphql.Internal;
import graphql.language.NamedNode;
import graphql.language.NodeParentTree;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLDirectiveContainer;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumValueDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLUnionType;
import graphql.schema.GraphqlElementParentTree;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static graphql.Assert.assertNotNull;
import static java.util.stream.Collectors.toList;

/**
 * This contains the helper code that allows {@link graphql.schema.idl.SchemaDirectiveWiring} implementations
 * to be invoked during schema generation.
 */
@Internal
public class SchemaGeneratorDirectiveHelper {

    static class Parameters {
        private final TypeDefinitionRegistry typeRegistry;
        private final RuntimeWiring runtimeWiring;
        private final NodeParentTree<NamedNode> nodeParentTree;
        private final Map<String, Object> context;
        private final GraphQLCodeRegistry.Builder codeRegistry;
        private final GraphqlElementParentTree elementParentTree;
        private final GraphQLFieldsContainer fieldsContainer;
        private final GraphQLFieldDefinition fieldDefinition;

        Parameters(TypeDefinitionRegistry typeRegistry, RuntimeWiring runtimeWiring, Map<String, Object> context, GraphQLCodeRegistry.Builder codeRegistry) {
            this(typeRegistry, runtimeWiring, context, codeRegistry, null, null, null, null);
        }

        Parameters(TypeDefinitionRegistry typeRegistry, RuntimeWiring runtimeWiring, Map<String, Object> context, GraphQLCodeRegistry.Builder codeRegistry, NodeParentTree<NamedNode> nodeParentTree, GraphqlElementParentTree elementParentTree, GraphQLFieldsContainer fieldsContainer, GraphQLFieldDefinition fieldDefinition) {
            this.typeRegistry = typeRegistry;
            this.runtimeWiring = runtimeWiring;
            this.nodeParentTree = nodeParentTree;
            this.context = context;
            this.codeRegistry = codeRegistry;
            this.elementParentTree = elementParentTree;
            this.fieldsContainer = fieldsContainer;
            this.fieldDefinition = fieldDefinition;
        }

        public TypeDefinitionRegistry getTypeRegistry() {
            return typeRegistry;
        }

        public RuntimeWiring getRuntimeWiring() {
            return runtimeWiring;
        }

        public NodeParentTree<NamedNode> getNodeParentTree() {
            return nodeParentTree;
        }

        public GraphqlElementParentTree getElementParentTree() {
            return elementParentTree;
        }

        public GraphQLFieldsContainer getFieldsContainer() {
            return fieldsContainer;
        }

        public Map<String, Object> getContext() {
            return context;
        }

        public GraphQLCodeRegistry.Builder getCodeRegistry() {
            return codeRegistry;
        }

        public GraphQLFieldDefinition getFieldsDefinition() {
            return fieldDefinition;
        }

        public Parameters newParams(GraphQLFieldsContainer fieldsContainer, NodeParentTree<NamedNode> nodeParentTree, GraphqlElementParentTree elementParentTree) {
            return new Parameters(this.typeRegistry, this.runtimeWiring, this.context, this.codeRegistry, nodeParentTree, elementParentTree, fieldsContainer, fieldDefinition);
        }

        public Parameters newParams(GraphQLFieldDefinition fieldDefinition, GraphQLFieldsContainer fieldsContainer, NodeParentTree<NamedNode> nodeParentTree, GraphqlElementParentTree elementParentTree) {
            return new Parameters(this.typeRegistry, this.runtimeWiring, this.context, this.codeRegistry, nodeParentTree, elementParentTree, fieldsContainer, fieldDefinition);
        }

        public Parameters newParams(NodeParentTree<NamedNode> nodeParentTree, GraphqlElementParentTree elementParentTree) {
            return new Parameters(this.typeRegistry, this.runtimeWiring, this.context, this.codeRegistry, nodeParentTree, elementParentTree, this.fieldsContainer, fieldDefinition);
        }
    }

    private NodeParentTree<NamedNode> buildAstTree(NamedNode... nodes) {
        Deque<NamedNode> nodeStack = new ArrayDeque<>();
        for (NamedNode node : nodes) {
            nodeStack.push(node);
        }
        return new NodeParentTree<>(nodeStack);
    }

    private GraphqlElementParentTree buildRuntimeTree(GraphQLType... elements) {
        Deque<GraphQLType> nodeStack = new ArrayDeque<>();
        for (GraphQLType element : elements) {
            nodeStack.push(element);
        }
        return new GraphqlElementParentTree(nodeStack);
    }

    private List<GraphQLArgument> wireArguments(GraphQLFieldDefinition fieldDefinition, GraphQLFieldsContainer fieldsContainer, NamedNode fieldsContainerNode, Parameters params, GraphQLFieldDefinition field) {
        return field.getArguments().stream().map(argument -> {

            NodeParentTree<NamedNode> nodeParentTree = buildAstTree(fieldsContainerNode, field.getDefinition(), argument.getDefinition());
            GraphqlElementParentTree elementParentTree = buildRuntimeTree(fieldsContainer, field, argument);

            Parameters argParams = params.newParams(fieldDefinition, fieldsContainer, nodeParentTree, elementParentTree);

            return onArgument(argument, argParams);
        }).collect(toList());
    }

    private List<GraphQLFieldDefinition> wireFields(GraphQLFieldsContainer fieldsContainer, NamedNode fieldsContainerNode, Parameters params) {
        return fieldsContainer.getFieldDefinitions().stream().map(fieldDefinition -> {

            // and for each argument in the fieldDefinition run the wiring for them - and note that they can change
            List<GraphQLArgument> newArgs = wireArguments(fieldDefinition, fieldsContainer, fieldsContainerNode, params, fieldDefinition);

            // they may have changed the arguments to the fieldDefinition so reflect that
            fieldDefinition = fieldDefinition.transform(builder -> builder.clearArguments().arguments(newArgs));

            NodeParentTree<NamedNode> nodeParentTree = buildAstTree(fieldsContainerNode, fieldDefinition.getDefinition());
            GraphqlElementParentTree elementParentTree = buildRuntimeTree(fieldsContainer, fieldDefinition);
            Parameters fieldParams = params.newParams(fieldDefinition, fieldsContainer, nodeParentTree, elementParentTree);

            // now for each fieldDefinition run the new wiring and capture the results
            return onField(fieldDefinition, fieldParams);
        }).collect(toList());
    }


    public GraphQLObjectType onObject(final GraphQLObjectType objectType, Parameters params) {
        List<GraphQLFieldDefinition> newFields = wireFields(objectType, objectType.getDefinition(), params);

        GraphQLObjectType newObjectType = objectType.transform(builder -> builder.clearFields().fields(newFields));

        NodeParentTree<NamedNode> nodeParentTree = buildAstTree(newObjectType.getDefinition());
        GraphqlElementParentTree elementParentTree = buildRuntimeTree(newObjectType);
        Parameters newParams = params.newParams(newObjectType, nodeParentTree, elementParentTree);

        return wireDirectives(params, newObjectType, newObjectType.getDirectives(),
                (outputElement, directives, registeredDirective) -> new SchemaDirectiveWiringEnvironmentImpl<>(outputElement, directives, registeredDirective, newParams), SchemaDirectiveWiring::onObject);
    }

    public GraphQLInterfaceType onInterface(GraphQLInterfaceType interfaceType, Parameters params) {
        List<GraphQLFieldDefinition> newFields = wireFields(interfaceType, interfaceType.getDefinition(), params);

        GraphQLInterfaceType newInterfaceType = interfaceType.transform(builder -> builder.clearFields().fields(newFields));

        NodeParentTree<NamedNode> nodeParentTree = buildAstTree(newInterfaceType.getDefinition());
        GraphqlElementParentTree elementParentTree = buildRuntimeTree(newInterfaceType);
        Parameters newParams = params.newParams(newInterfaceType, nodeParentTree, elementParentTree);

        return wireDirectives(params, newInterfaceType, newInterfaceType.getDirectives(),
                (outputElement, directives, registeredDirective) -> new SchemaDirectiveWiringEnvironmentImpl<>(outputElement, directives, registeredDirective, newParams), SchemaDirectiveWiring::onInterface);
    }

    public GraphQLEnumType onEnum(GraphQLEnumType enumType, Parameters params) {

        List<GraphQLEnumValueDefinition> newEnums = enumType.getValues().stream().map(enumValueDefinition -> {

            NodeParentTree<NamedNode> nodeParentTree = buildAstTree(enumType.getDefinition(), enumValueDefinition.getDefinition());
            GraphqlElementParentTree elementParentTree = buildRuntimeTree(enumType, enumValueDefinition);
            Parameters fieldParams = params.newParams(nodeParentTree, elementParentTree);

            // now for each field run the new wiring and capture the results
            return onEnumValue(enumValueDefinition, fieldParams);
        }).collect(toList());

        GraphQLEnumType newEnumType = enumType.transform(builder -> builder.clearValues().values(newEnums));

        NodeParentTree<NamedNode> nodeParentTree = buildAstTree(newEnumType.getDefinition());
        GraphqlElementParentTree elementParentTree = buildRuntimeTree(newEnumType);
        Parameters newParams = params.newParams(nodeParentTree, elementParentTree);

        return wireDirectives(params, newEnumType, newEnumType.getDirectives(),
                (outputElement, directives, registeredDirective) -> new SchemaDirectiveWiringEnvironmentImpl<>(outputElement, directives, registeredDirective, newParams), SchemaDirectiveWiring::onEnum);
    }

    public GraphQLInputObjectType onInputObjectType(GraphQLInputObjectType inputObjectType, Parameters params) {
        List<GraphQLInputObjectField> newFields = inputObjectType.getFieldDefinitions().stream().map(inputField -> {

            NodeParentTree<NamedNode> nodeParentTree = buildAstTree(inputObjectType.getDefinition(), inputField.getDefinition());
            GraphqlElementParentTree elementParentTree = buildRuntimeTree(inputObjectType, inputField);
            Parameters fieldParams = params.newParams(nodeParentTree, elementParentTree);

            // now for each field run the new wiring and capture the results
            return onInputObjectField(inputField, fieldParams);
        }).collect(toList());

        GraphQLInputObjectType newInputObjectType = inputObjectType.transform(builder -> builder.clearFields().fields(newFields));

        NodeParentTree<NamedNode> nodeParentTree = buildAstTree(newInputObjectType.getDefinition());
        GraphqlElementParentTree elementParentTree = buildRuntimeTree(newInputObjectType);
        Parameters newParams = params.newParams(nodeParentTree, elementParentTree);

        return wireDirectives(params, newInputObjectType, newInputObjectType.getDirectives(),
                (outputElement, directives, registeredDirective) -> new SchemaDirectiveWiringEnvironmentImpl<>(outputElement, directives, registeredDirective, newParams), SchemaDirectiveWiring::onInputObjectType);
    }


    public GraphQLUnionType onUnion(GraphQLUnionType element, Parameters params) {
        NodeParentTree<NamedNode> nodeParentTree = buildAstTree(element.getDefinition());
        GraphqlElementParentTree elementParentTree = buildRuntimeTree(element);
        Parameters newParams = params.newParams(nodeParentTree, elementParentTree);

        return wireDirectives(params, element, element.getDirectives(),
                (outputElement, directives, registeredDirective) -> new SchemaDirectiveWiringEnvironmentImpl<>(outputElement, directives, registeredDirective, newParams), SchemaDirectiveWiring::onUnion);
    }

    public GraphQLScalarType onScalar(GraphQLScalarType element, Parameters params) {
        NodeParentTree<NamedNode> nodeParentTree = buildAstTree(element.getDefinition());
        GraphqlElementParentTree elementParentTree = buildRuntimeTree(element);
        Parameters newParams = params.newParams(nodeParentTree, elementParentTree);

        return wireDirectives(params, element, element.getDirectives(),
                (outputElement, directives, registeredDirective) -> new SchemaDirectiveWiringEnvironmentImpl<>(outputElement, directives, registeredDirective, newParams), SchemaDirectiveWiring::onScalar);
    }

    private GraphQLFieldDefinition onField(GraphQLFieldDefinition fieldDefinition, Parameters params) {
        return wireDirectives(params, fieldDefinition, fieldDefinition.getDirectives(),
                (outputElement, directives, registeredDirective) -> new SchemaDirectiveWiringEnvironmentImpl<>(outputElement, directives, registeredDirective, params), SchemaDirectiveWiring::onField);
    }

    private GraphQLInputObjectField onInputObjectField(GraphQLInputObjectField element, Parameters params) {
        return wireDirectives(params, element, element.getDirectives(),
                (outputElement, directives, registeredDirective) -> new SchemaDirectiveWiringEnvironmentImpl<>(outputElement, directives, registeredDirective, params), SchemaDirectiveWiring::onInputObjectField);
    }

    private GraphQLEnumValueDefinition onEnumValue(GraphQLEnumValueDefinition enumValueDefinition, Parameters params) {
        return wireDirectives(params, enumValueDefinition, enumValueDefinition.getDirectives(),
                (outputElement, directives, registeredDirective) -> new SchemaDirectiveWiringEnvironmentImpl<>(outputElement, directives, registeredDirective, params), SchemaDirectiveWiring::onEnumValue);
    }

    private GraphQLArgument onArgument(GraphQLArgument argument, Parameters params) {
        return wireDirectives(params, argument, argument.getDirectives(),
                (outputElement, directives, registeredDirective) -> new SchemaDirectiveWiringEnvironmentImpl<>(outputElement, directives, registeredDirective, params), SchemaDirectiveWiring::onArgument);
    }


    //
    // builds a type safe SchemaDirectiveWiringEnvironment
    //
    interface EnvBuilder<T extends GraphQLDirectiveContainer> {
        SchemaDirectiveWiringEnvironment<T> apply(T outputElement, List<GraphQLDirective> allDirectives, GraphQLDirective registeredDirective);
    }

    //
    // invokes the SchemaDirectiveWiring with the provided environment
    //
    interface EnvInvoker<T extends GraphQLDirectiveContainer> {
        T apply(SchemaDirectiveWiring schemaDirectiveWiring, SchemaDirectiveWiringEnvironment<T> env);
    }

    private <T extends GraphQLDirectiveContainer> T wireDirectives(
            Parameters parameters, T element,
            List<GraphQLDirective> allDirectives,
            EnvBuilder<T> envBuilder,
            EnvInvoker<T> invoker) {

        RuntimeWiring runtimeWiring = parameters.getRuntimeWiring();
        WiringFactory wiringFactory = runtimeWiring.getWiringFactory();
        SchemaDirectiveWiring schemaDirectiveWiring;

        SchemaDirectiveWiringEnvironment<T> env;
        T outputObject = element;
        //
        // first the specific named directives
        Map<String, SchemaDirectiveWiring> mapOfWiring = runtimeWiring.getRegisteredDirectiveWiring();
        for (GraphQLDirective directive : allDirectives) {
            schemaDirectiveWiring = mapOfWiring.get(directive.getName());
            if (schemaDirectiveWiring != null) {
                env = envBuilder.apply(outputObject, allDirectives, directive);
                outputObject = invokeWiring(outputObject, invoker, schemaDirectiveWiring, env);
            }
        }
        //
        // now call any statically added to the the runtime
        for (SchemaDirectiveWiring directiveWiring : runtimeWiring.getDirectiveWiring()) {
            env = envBuilder.apply(outputObject, allDirectives, null);
            outputObject = invokeWiring(outputObject, invoker, directiveWiring, env);
        }
        //
        // wiring factory is last (if present)
        env = envBuilder.apply(outputObject, allDirectives, null);
        if (wiringFactory.providesSchemaDirectiveWiring(env)) {
            schemaDirectiveWiring = assertNotNull(wiringFactory.getSchemaDirectiveWiring(env), "Your WiringFactory MUST provide a non null SchemaDirectiveWiring");
            outputObject = invokeWiring(outputObject, invoker, schemaDirectiveWiring, env);
        }

        return outputObject;
    }

    private <T extends GraphQLDirectiveContainer> T invokeWiring(T element, EnvInvoker<T> invoker, SchemaDirectiveWiring schemaDirectiveWiring, SchemaDirectiveWiringEnvironment<T> env) {
        T newElement = invoker.apply(schemaDirectiveWiring, env);
        assertNotNull(newElement, "The SchemaDirectiveWiring MUST return a non null return value for element '" + element.getName() + "'");
        return newElement;
    }
}
