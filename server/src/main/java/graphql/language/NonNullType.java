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
public class NonNullType extends AbstractNode<NonNullType> implements Type<NonNullType> {

    private final Type type;

    public static final String CHILD_TYPE = "type";

    @Internal
    protected NonNullType(Type type, SourceLocation sourceLocation, List<Comment> comments, IgnoredChars ignoredChars, Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.type = type;
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param type the wrapped type
     */
    public NonNullType(Type type) {
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
    public NonNullType withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .type((Type) newChildren.getChildOrNull(CHILD_TYPE))
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
    public NonNullType deepCopy() {
        return new NonNullType(deepCopy(type), getSourceLocation(), getComments(), getIgnoredChars(), getAdditionalData());
    }

    @Override
    public String toString() {
        return "NonNullType{" +
                "type=" + type +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitNonNullType(this, context);
    }

    public static Builder newNonNullType() {
        return new Builder();
    }

    public static Builder newNonNullType(Type type) {
        return new Builder().type(type);
    }

    public NonNullType transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private Type type;
        private List<Comment> comments = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(NonNullType existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.type = existing.getType();
            this.ignoredChars = existing.getIgnoredChars();
            this.additionalData = existing.getAdditionalData();
        }


        public Builder sourceLocation(SourceLocation sourceLocation) {
            this.sourceLocation = sourceLocation;
            return this;
        }

        public Builder type(ListType type) {
            this.type = type;
            return this;
        }

        public Builder type(TypeName type) {
            this.type = type;
            return this;
        }

        public Builder type(Type type) {
            if (!(type instanceof ListType) && !(type instanceof TypeName)) {
                throw new IllegalArgumentException("unexpected type");
            }
            this.type = type;
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


        public NonNullType build() {
            return new NonNullType(type, sourceLocation, comments, ignoredChars, additionalData);
        }
    }
}
