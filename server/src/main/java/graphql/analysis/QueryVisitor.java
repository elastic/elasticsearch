package graphql.analysis;

import graphql.PublicApi;
import graphql.util.TraversalControl;

/**
 * Used by {@link QueryTraverser} to visit the nodes of a Query.
 * <p>
 * How this happens in detail (pre vs post-order for example) is defined by {@link QueryTraverser}.
 */
@PublicApi
public interface QueryVisitor {

    void visitField(QueryVisitorFieldEnvironment queryVisitorFieldEnvironment);

    /**
     * visitField variant which lets you control the traversal.
     * default implementation calls visitField for backwards compatibility reason
     *
     * @param queryVisitorFieldEnvironment
     *
     * @return
     */
    default TraversalControl visitFieldWithControl(QueryVisitorFieldEnvironment queryVisitorFieldEnvironment) {
        visitField(queryVisitorFieldEnvironment);
        return TraversalControl.CONTINUE;
    }

    void visitInlineFragment(QueryVisitorInlineFragmentEnvironment queryVisitorInlineFragmentEnvironment);

    void visitFragmentSpread(QueryVisitorFragmentSpreadEnvironment queryVisitorFragmentSpreadEnvironment);

    default void visitFragmentDefinition(QueryVisitorFragmentDefinitionEnvironment queryVisitorFragmentDefinitionEnvironment) {

    }

}
