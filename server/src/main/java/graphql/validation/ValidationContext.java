package graphql.validation;


import graphql.Internal;
import graphql.language.Definition;
import graphql.language.Document;
import graphql.language.FragmentDefinition;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCompositeType;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Internal
public class ValidationContext {

    private final GraphQLSchema schema;
    private final Document document;

    private final TraversalContext traversalContext;
    private final Map<String, FragmentDefinition> fragmentDefinitionMap = new LinkedHashMap<>();


    public ValidationContext(GraphQLSchema schema, Document document) {
        this.schema = schema;
        this.document = document;
        this.traversalContext = new TraversalContext(schema);
        buildFragmentMap();
    }

    private void buildFragmentMap() {
        for (Definition definition : document.getDefinitions()) {
            if (!(definition instanceof FragmentDefinition)) continue;
            FragmentDefinition fragmentDefinition = (FragmentDefinition) definition;
            fragmentDefinitionMap.put(fragmentDefinition.getName(), fragmentDefinition);
        }
    }

    public TraversalContext getTraversalContext() {
        return traversalContext;
    }

    public GraphQLSchema getSchema() {
        return schema;
    }

    public Document getDocument() {
        return document;
    }

    public FragmentDefinition getFragment(String name) {
        return fragmentDefinitionMap.get(name);
    }

    public GraphQLCompositeType getParentType() {
        return traversalContext.getParentType();
    }

    public GraphQLInputType getInputType() {
        return traversalContext.getInputType();
    }

    public GraphQLFieldDefinition getFieldDef() {
        return traversalContext.getFieldDef();
    }

    public GraphQLDirective getDirective() {
        return traversalContext.getDirective();
    }

    public GraphQLArgument getArgument() {
        return traversalContext.getArgument();
    }

    public GraphQLOutputType getOutputType() {
        return traversalContext.getOutputType();
    }


    public List<String> getQueryPath() {
        return traversalContext.getQueryPath();
    }

    @Override
    public String toString() {
        return "ValidationContext{" + getQueryPath() + "}";
    }
}
