package graphql.analysis;

import graphql.Internal;
import graphql.language.InlineFragment;
import graphql.language.Node;
import graphql.util.TraverserContext;

import java.util.Objects;

@Internal
public class QueryVisitorInlineFragmentEnvironmentImpl implements QueryVisitorInlineFragmentEnvironment {
    private final InlineFragment inlineFragment;
    private final TraverserContext<Node> traverserContext;

    public QueryVisitorInlineFragmentEnvironmentImpl(InlineFragment inlineFragment, TraverserContext<Node> traverserContext) {
        this.inlineFragment = inlineFragment;
        this.traverserContext = traverserContext;
    }

    @Override
    public InlineFragment getInlineFragment() {
        return inlineFragment;
    }

    @Override
    public TraverserContext<Node> getTraverserContext() {
        return traverserContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryVisitorInlineFragmentEnvironmentImpl that = (QueryVisitorInlineFragmentEnvironmentImpl) o;
        return Objects.equals(inlineFragment, that.inlineFragment);
    }

    @Override
    public int hashCode() {

        return Objects.hash(inlineFragment);
    }

    @Override
    public String toString() {
        return "QueryVisitorInlineFragmentEnvironmentImpl{" +
                "inlineFragment=" + inlineFragment +
                '}';
    }
}
