package graphql.language;

import graphql.PublicApi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;

/**
 * Container of children of a {@link Node}.
 */
@PublicApi
public class NodeChildrenContainer {

    private final Map<String, List<Node>> children = new LinkedHashMap<>();

    private NodeChildrenContainer(Map<String, List<Node>> children) {
        this.children.putAll(assertNotNull(children));
    }

    public <T extends Node> List<T> getChildren(String key) {
        return (List<T>) children.getOrDefault(key, new ArrayList<>());
    }

    public <T extends Node> T getChildOrNull(String key) {
        List<? extends Node> result = children.getOrDefault(key, new ArrayList<>());
        if (result.size() > 1) {
            throw new IllegalStateException("children " + key + " is not a single value");
        }
        return result.size() > 0 ? (T) result.get(0) : null;
    }

    public Map<String, List<Node>> getChildren() {
        return new LinkedHashMap<>(children);
    }

    public static Builder newNodeChildrenContainer() {
        return new Builder();
    }

    public static Builder newNodeChildrenContainer(Map<String, ? extends List<? extends Node>> childrenMap) {
        return new Builder().children(childrenMap);
    }

    public static Builder newNodeChildrenContainer(NodeChildrenContainer existing) {
        return new Builder(existing);
    }

    public NodeChildrenContainer transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public boolean isEmpty() {
        return this.children.isEmpty();
    }

    public static class Builder {
        private final Map<String, List<Node>> children = new LinkedHashMap<>();

        private Builder() {

        }

        private Builder(NodeChildrenContainer other) {
            this.children.putAll(other.children);
        }

        public Builder child(String key, Node child) {
            // we allow null here to make the actual nodes easier
            if (child == null) {
                return this;
            }
            children.computeIfAbsent(key, (k) -> new ArrayList<>());
            children.get(key).add(child);
            return this;
        }

        public Builder children(String key, List<? extends Node> children) {
            this.children.computeIfAbsent(key, (k) -> new ArrayList<>());
            this.children.get(key).addAll(children);
            return this;
        }

        public Builder children(Map<String, ? extends List<? extends Node>> children) {
            this.children.clear();
            this.children.putAll((Map<? extends String, ? extends List<Node>>) children);
            return this;
        }

        public Builder replaceChild(String key, int index, Node newChild) {
            assertNotNull(newChild);
            this.children.get(key).set(index, newChild);
            return this;
        }

        public Builder removeChild(String key, int index) {
            this.children.get(key).remove(index);
            return this;
        }

        public NodeChildrenContainer build() {
            return new NodeChildrenContainer(this.children);

        }
    }
}
