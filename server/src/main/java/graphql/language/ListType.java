package graphql.language;


import graphql.Internal;
import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;
import static graphql.language.NodeChildrenContainer.newNodeChildrenContainer;
import static java.util.Collections.emptyMap;

@PublicApi
public class ListType extends AbstractNode<ListType> implements Type<ListType> {

    private final Type type;

    public static final String CHILD_TYPE = "type";

    @Internal
    protected ListType(Type type, SourceLocation sourceLocation, List<Comment> comments, IgnoredChars ignoredChars, Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.type = type;
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param type the wrapped type
     */
    public ListType(Type type) {
        this(type, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    public Type getType() {
        return type;
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.add(type);
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .child(CHILD_TYPE, type)
                .build();
    }

    @Override
    public ListType withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .type(newChildren.getChildOrNull(CHILD_TYPE))
        );
    }

    @Override
    public boolean isEqualTo(Node o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public ListType deepCopy() {
        return new ListType(deepCopy(type), getSourceLocation(), getComments(), getIgnoredChars(), getAdditionalData());
    }

    @Override
    public String toString() {
        return "ListType{" +
                "type=" + type +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitListType(this, context);
    }

    public static Builder newListType() {
        return new Builder();
    }

    public static Builder newListType(Type type) {
        return new Builder().type(type);
    }

    public ListType transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private Type type;
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(ListType existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.type = existing.getType();
            this.ignoredChars = existing.getIgnoredChars();
            this.additionalData = existing.getAdditionalData();
        }


        public Builder type(Type type) {
            this.type = type;
            return this;
        }

        public Builder sourceLocation(SourceLocation sourceLocation) {
            this.sourceLocation = sourceLocation;
            return this;
        }

        public Builder comments(List<Comment> comments) {
            this.comments = comments;
            return this;
        }

        public Builder ignoredChars(IgnoredChars ignoredChars) {
            this.ignoredChars = ignoredChars;
            return this;
        }

        public Builder additionalData(Map<String, String> additionalData) {
            this.additionalData = assertNotNull(additionalData);
            return this;
        }

        public Builder additionalData(String key, String value) {
            this.additionalData.put(key, value);
            return this;
        }

        public ListType build() {
            return new ListType(type, sourceLocation, comments, ignoredChars, additionalData);
        }
    }
}
