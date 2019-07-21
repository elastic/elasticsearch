package graphql.language;

import graphql.Internal;
import graphql.PublicApi;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertTrue;

/**
 * This represents a hierarchy from a graphql language node upwards to its
 * associated parent nodes.  For example a Directive can be on a InputValueDefinition
 * which can be on a Argument, which can be on a FieldDefinition which may be
 * on an ObjectTypeDefinition.
 */
@PublicApi
public class NodeParentTree<T extends Node> {

    private final T node;
    private final NodeParentTree<T> parent;
    private final List<String> path;

    @Internal
    public NodeParentTree(Deque<T> nodeStack) {
        assertNotNull(nodeStack, "You MUST have a non null stack of nodes");
        assertTrue(!nodeStack.isEmpty(), "You MUST have a non empty stack of nodes");

        Deque<T> copy = new ArrayDeque<>(nodeStack);
        path = mkPath(copy);
        node = copy.pop();
        if (!copy.isEmpty()) {
            parent = new NodeParentTree<T>(copy);
        } else {
            parent = null;
        }
    }

    private List<String> mkPath(Deque<T> copy) {
        return copy.stream()
                .filter(node1 -> node1 instanceof NamedNode)
                .map(node1 -> ((NamedNode) node1).getName())
                .collect(Collectors.toList());
    }


    /**
     * Returns the node represented by this info
     *
     * @return the node in play
     */
    public T getNode() {
        return node;
    }

    /**
     * @return a node MAY have an optional parent
     */
    public Optional<NodeParentTree<T>> getParentInfo() {
        return Optional.ofNullable(parent);
    }

    /**
     * @return a path of names for nodes thar are {@link graphql.language.NamedNode}s
     */
    public List<String> getPath() {
        return path;
    }

    /**
     * @return the tree as a list of T
     */
    public List<T> toList() {
        List<T> nodes = new ArrayList<>();
        nodes.add(node);
        Optional<NodeParentTree<T>> parentInfo = this.getParentInfo();
        while (parentInfo.isPresent()) {
            nodes.add(parentInfo.get().getNode());
            parentInfo = parentInfo.get().getParentInfo();
        }
        return nodes;
    }

    @Override
    public String toString() {
        return String.valueOf(node) +
                " - parent : " +
                parent;
    }
}