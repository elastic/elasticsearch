package graphql.analysis;

import graphql.PublicApi;
import graphql.language.FragmentDefinition;
import graphql.language.Node;
import graphql.util.TraverserContext;

@PublicApi
public interface QueryVisitorFragmentDefinitionEnvironment {
    FragmentDefinition getFragmentDefinition();

    TraverserContext<Node> getTraverserContext();
}
