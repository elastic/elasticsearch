package graphql.language;

import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import graphql.util.TraverserVisitor;
import graphql.util.TreeTransformer;

import static graphql.Assert.assertNotNull;
import static graphql.language.AstNodeAdapter.AST_NODE_ADAPTER;

/**
 * Allows for an easy way to "manipulate" the immutable Ast by changing specific nodes and getting back a new Ast
 * containing the changed nodes while everything else is the same.
 */
@PublicApi
public class AstTransformer {


    public Node transform(Node root, NodeVisitor nodeVisitor) {
        assertNotNull(root);
        assertNotNull(nodeVisitor);

        TraverserVisitor<Node> traverserVisitor = new TraverserVisitor<Node>() {
            @Override
            public TraversalControl enter(TraverserContext<Node> context) {
                return context.thisNode().accept(context, nodeVisitor);
            }

            @Override
            public TraversalControl leave(TraverserContext<Node> context) {
                return TraversalControl.CONTINUE;
            }
        };

        TreeTransformer<Node> treeTransformer = new TreeTransformer<>(AST_NODE_ADAPTER);
        return treeTransformer.transform(root, traverserVisitor);
    }


}
