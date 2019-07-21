package graphql.analysis;

import graphql.PublicApi;
import graphql.language.InlineFragment;
import graphql.language.Node;
import graphql.util.TraverserContext;

@PublicApi
public interface QueryVisitorInlineFragmentEnvironment {
    InlineFragment getInlineFragment();

    TraverserContext<Node> getTraverserContext();
}
