package graphql.language;


import graphql.Internal;
import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;
import static graphql.language.NodeChildrenContainer.newNodeChildrenContainer;
import static graphql.language.NodeUtil.assertNewChildrenAreEmpty;

@PublicApi
public class StringValue extends AbstractNode<StringValue> implements ScalarValue<StringValue> {

    private final String value;

    @Internal
    protected StringValue(String value, SourceLocation sourceLocation, List<Comment> comments, IgnoredChars ignoredChars, Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.value = value;
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param value of the String
     */
    public StringValue(String value) {
        this(value, null, new ArrayList<>(), IgnoredChars.EMPTY, Collections.emptyMap());
    }

    public String getValue() {
        return value;
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer().build();
    }

    @Override
    public StringValue withNewChildren(NodeChildrenContainer newChildren) {
        assertNewChildrenAreEmpty(newChildren);
        return this;
    }

    @Override
    public String toString() {
        return "StringValue{" +
                "value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean isEqualTo(Node o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StringValue that = (StringValue) o;

        return !(value != null ? !value.equals(that.value) : that.value != null);

    }

    @Override
    public StringValue deepCopy() {
        return new StringValue(value, getSourceLocation(), getComments(), getIgnoredChars(), getAdditionalData());
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitStringValue(this, context);
    }

    public static Builder newStringValue() {
        return new Builder();
    }

    public static Builder newStringValue(String value) {
        return new Builder().value(value);
    }

    public StringValue transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private String value;
        private List<Comment> comments = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(StringValue existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.value = existing.getValue();
            this.ignoredChars = existing.getIgnoredChars();
            this.additionalData = existing.getAdditionalData();
        }


        public Builder sourceLocation(SourceLocation sourceLocation) {
            this.sourceLocation = sourceLocation;
            return this;
        }

        public Builder value(String value) {
            this.value = value;
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


        public StringValue build() {
            StringValue stringValue = new StringValue(value, sourceLocation, comments, ignoredChars, additionalData);
            return stringValue;
        }
    }
}
