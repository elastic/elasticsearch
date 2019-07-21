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
import java.util.stream.Collectors;

import static graphql.Assert.assertNotNull;
import static graphql.language.NodeChildrenContainer.newNodeChildrenContainer;
import static java.util.Collections.emptyMap;

@PublicApi
public class Document extends AbstractNode<Document> {

    private final List<Definition> definitions;

    public static final String CHILD_DEFINITIONS = "definitions";

    @Internal
    protected Document(List<Definition> definitions, SourceLocation sourceLocation, List<Comment> comments, IgnoredChars ignoredChars, Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.definitions = definitions;
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param definitions the definitions that make up this document
     */
    public Document(List<Definition> definitions) {
        this(definitions, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    public List<Definition> getDefinitions() {
        return new ArrayList<>(definitions);
    }

    /**
     * Returns a list of definitions of the specific type.  It uses {@link java.lang.Class#isAssignableFrom(Class)} for the test
     *
     * @param definitionClass the definition class
     * @param <T>             the type of definition
     *
     * @return a list of definitions of that class or empty list
     */
    public <T extends Definition> List<T> getDefinitionsOfType(Class<T> definitionClass) {
        return definitions.stream()
                .filter(d -> definitionClass.isAssignableFrom(d.getClass()))
                .map(definitionClass::cast)
                .collect(Collectors.toList());
    }

    @Override
    public List<Node> getChildren() {
        return new ArrayList<>(definitions);
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .children(CHILD_DEFINITIONS, definitions)
                .build();
    }

    @Override
    public Document withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .definitions(newChildren.getChildren(CHILD_DEFINITIONS))
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
    public Document deepCopy() {
        return new Document(deepCopy(definitions), getSourceLocation(), getComments(), getIgnoredChars(), getAdditionalData());
    }

    @Override
    public String toString() {
        return "Document{" +
                "definitions=" + definitions +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitDocument(this, context);
    }

    public static Builder newDocument() {
        return new Builder();
    }

    public Document transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private List<Definition> definitions = new ArrayList<>();
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(Document existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.definitions = existing.getDefinitions();
            this.ignoredChars = existing.getIgnoredChars();
            this.additionalData = existing.getAdditionalData();
        }

        public Builder definitions(List<Definition> definitions) {
            this.definitions = definitions;
            return this;
        }

        public Builder definition(Definition definition) {
            this.definitions.add(definition);
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


        public Document build() {
            return new Document(definitions, sourceLocation, comments, ignoredChars, additionalData);
        }
    }
}
