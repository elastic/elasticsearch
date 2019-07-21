package graphql.schema.idl;

import graphql.Internal;
import graphql.language.NamedNode;
import graphql.language.NodeParentTree;
import graphql.schema.DataFetcher;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLDirectiveContainer;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphqlElementParentTree;
import graphql.util.FpKit;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static graphql.Assert.assertNotNull;

@Internal
public class SchemaDirectiveWiringEnvironmentImpl<T extends GraphQLDirectiveContainer> implements SchemaDirectiveWiringEnvironment<T> {

    private final T element;
    private final Map<String, GraphQLDirective> directives;
    private final NodeParentTree<NamedNode> nodeParentTree;
    private final TypeDefinitionRegistry typeDefinitionRegistry;
    private final Map<String, Object> context;
    private final GraphQLCodeRegistry.Builder codeRegistry;
    private final GraphqlElementParentTree elementParentTree;
    private final GraphQLFieldsContainer fieldsContainer;
    private final GraphQLFieldDefinition fieldDefinition;
    private final GraphQLDirective registeredDirective;

    public SchemaDirectiveWiringEnvironmentImpl(T element, List<GraphQLDirective> directives, GraphQLDirective registeredDirective, SchemaGeneratorDirectiveHelper.Parameters parameters) {
        this.element = element;
        this.registeredDirective = registeredDirective;
        this.typeDefinitionRegistry = parameters.getTypeRegistry();
        this.directives = FpKit.getByName(directives, GraphQLDirective::getName);
        this.context = parameters.getContext();
        this.codeRegistry = parameters.getCodeRegistry();
        this.nodeParentTree = parameters.getNodeParentTree();
        this.elementParentTree = parameters.getElementParentTree();
        this.fieldsContainer = parameters.getFieldsContainer();
        this.fieldDefinition = parameters.getFieldsDefinition();
    }

    @Override
    public T getElement() {
        return element;
    }

    @Override
    public GraphQLDirective getDirective() {
        return registeredDirective;
    }

    @Override
    public Map<String, GraphQLDirective> getDirectives() {
        return new LinkedHashMap<>(directives);
    }

    @Override
    public GraphQLDirective getDirective(String directiveName) {
        return directives.get(directiveName);
    }

    @Override
    public boolean containsDirective(String directiveName) {
        return directives.containsKey(directiveName);
    }

    @Override
    public NodeParentTree<NamedNode> getNodeParentTree() {
        return nodeParentTree;
    }

    @Override
    public TypeDefinitionRegistry getRegistry() {
        return typeDefinitionRegistry;
    }

    @Override
    public Map<String, Object> getBuildContext() {
        return context;
    }

    @Override
    public GraphQLCodeRegistry.Builder getCodeRegistry() {
        return codeRegistry;
    }

    @Override
    public GraphQLFieldsContainer getFieldsContainer() {
        return fieldsContainer;
    }

    @Override
    public GraphqlElementParentTree getElementParentTree() {
        return elementParentTree;
    }

    @Override
    public GraphQLFieldDefinition getFieldDefinition() {
        return fieldDefinition;
    }

    @Override
    public DataFetcher getFieldDataFetcher() {
        assertNotNull(fieldDefinition, "An output field must be in context to call this method");
        assertNotNull(fieldsContainer, "An output field container must be in context to call this method");
        return codeRegistry.getDataFetcher(fieldsContainer, fieldDefinition);
    }

    @Override
    public GraphQLFieldDefinition setFieldDataFetcher(DataFetcher newDataFetcher) {
        assertNotNull(fieldDefinition, "An output field must be in context to call this method");
        assertNotNull(fieldsContainer, "An output field container must be in context to call this method");

        FieldCoordinates coordinates = FieldCoordinates.coordinates(fieldsContainer, fieldDefinition);
        codeRegistry.dataFetcher(coordinates, newDataFetcher);
        return fieldDefinition;
    }
}
