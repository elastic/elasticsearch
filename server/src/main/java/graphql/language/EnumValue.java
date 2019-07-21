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
import static graphql.language.NodeUtil.assertNewChildrenAreEmpty;
import static java.util.Collections.emptyMap;

@PublicApi
public class EnumValue extends AbstractNode<EnumValue> implements Value<EnumValue>, NamedNode<EnumValue> {

    private final String name;

    @Internal
    protected EnumValue(String name, SourceLocation sourceLocation, List<Comment> comments, IgnoredChars ignoredChars, Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
    }


    /**
     * alternative to using a Builder for convenience
     *
     * @param name of the enum value
     */
    public EnumValue(String name) {
        this(name, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    @Override
    public String getName() {
        return name;
    }


    @Override
    public List<Node> getChildren() {
        return new ArrayList<>();
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer().build();
    }

    @Override
    public EnumValue withNewChildren(NodeChildrenContainer newChildren) {
        assertNewChildrenAreEmpty(newChildren);
        return this;
    }

    @Override
    public boolean isEqualTo(Node o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EnumValue that = (EnumValue) o;

        return NodeUtil.isEqualTo(this.name, that.name);
    }

    @Override
    public EnumValue deepCopy() {
        return new EnumValue(name, getSourceLocation(), getComments(), getIgnoredChars(), getAdditionalData());
    }

    @Override
    public String toString() {
        return "EnumValue{" +
                "name='" + name + '\'' +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitEnumValue(this, context);
    }

    public static Builder newEnumValue() {
        return new Builder();
    }

    public static Builder newEnumValue(String name) {
        return new Builder().name(name);
    }

    public EnumValue transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private String name;
        private List<Comment> comments = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(EnumValue existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.additionalData = existing.getAdditionalData();
        }


        public Builder sourceLocation(SourceLocation sourceLocation) {
            this.sourceLocation = sourceLocation;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
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


        public EnumValue build() {
            return new EnumValue(name, sourceLocation, comments, ignoredChars, additionalData);
        }
    }
}
