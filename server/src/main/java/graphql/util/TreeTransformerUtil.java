package graphql.util;

import graphql.PublicApi;

@PublicApi
public class TreeTransformerUtil {

    /**
     * Can be called multiple times to change the current node of the context. The latest call wins
     *
     * @param context
     * @param changedNode
     * @param <T>
     *
     * @return
     */
    public static <T> TraversalControl changeNode(TraverserContext<T> context, T changedNode) {
        NodeZipper<T> zipperWithChangedNode = context.getVar(NodeZipper.class).withNewNode(changedNode);
        NodeMultiZipper<T> multiZipper = context.getNewAccumulate();
        if (context.isChanged()) {
            context.setAccumulate(multiZipper.withReplacedZipperForNode(context.thisNode(), changedNode));
            context.changeNode(changedNode);
        } else {
            context.setAccumulate(multiZipper.withNewZipper(zipperWithChangedNode));
            context.changeNode(changedNode);
        }
        return TraversalControl.CONTINUE;
    }

    public static <T> TraversalControl deleteNode(TraverserContext<T> context) {
        NodeZipper<T> deleteNodeZipper = context.getVar(NodeZipper.class).deleteNode();
        NodeMultiZipper<T> multiZipper = context.getNewAccumulate();
        context.setAccumulate(multiZipper.withNewZipper(deleteNodeZipper));
        context.deleteNode();
        return TraversalControl.CONTINUE;
    }

    public static <T> TraversalControl insertAfter(TraverserContext<T> context, T toInsertAfter) {
        NodeZipper<T> insertNodeZipper = context.getVar(NodeZipper.class).insertAfter(toInsertAfter);
        NodeMultiZipper<T> multiZipper = context.getNewAccumulate();
        context.setAccumulate(multiZipper.withNewZipper(insertNodeZipper));
        return TraversalControl.CONTINUE;
    }

    public static <T> TraversalControl insertBefore(TraverserContext<T> context, T toInsertBefore) {
        NodeZipper<T> insertNodeZipper = context.getVar(NodeZipper.class).insertBefore(toInsertBefore);
        NodeMultiZipper<T> multiZipper = context.getNewAccumulate();
        context.setAccumulate(multiZipper.withNewZipper(insertNodeZipper));
        return TraversalControl.CONTINUE;
    }

}
