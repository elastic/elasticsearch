package graphql.language;


import graphql.Internal;
import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static graphql.Assert.assertNotNull;
import static graphql.language.NodeChildrenContainer.newNodeChildrenContainer;
import static java.util.Collections.emptyMap;

@PublicApi
public class SelectionSet extends AbstractNode<SelectionSet> {

    private final List<Selection> selections = new ArrayList<>();

    public static final String CHILD_SELECTIONS = "selections";

    @Internal
    protected SelectionSet(Collection<? extends Selection> selections, SourceLocation sourceLocation, List<Comment> comments, IgnoredChars ignoredChars, Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.selections.addAll(selections);
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param selections the list of selection in this selection set
     */
    public SelectionSet(Collection<? extends Selection> selections) {
        this(selections, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    public List<Selection> getSelections() {
        return new ArrayList<>(selections);
    }

    /**
     * Returns a list of selections of the specific type.  It uses {@link java.lang.Class#isAssignableFrom(Class)} for the test
     *
     * @param selectionClass the selection class
     * @param <T>            the type of selection
     *
     * @return a list of selections of that class or empty list
     */
    public <T extends Selection> List<T> getSelectionsOfType(Class<T> selectionClass) {
        return selections.stream()
                .filter(d -> selectionClass.isAssignableFrom(d.getClass()))
                .map(selectionClass::cast)
                .collect(Collectors.toList());
    }

    @Override
    public List<Node> getChildren() {
        return new ArrayList<>(selections);
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .children(CHILD_SELECTIONS, selections)
                .build();
    }

    @Override
    public SelectionSet withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .selections(newChildren.getChildren(CHILD_SELECTIONS))
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
    public SelectionSet deepCopy() {
        return new SelectionSet(deepCopy(selections), getSourceLocation(), getComments(), getIgnoredChars(), getAdditionalData());
    }

    @Override
    public String toString() {
        return "SelectionSet{" +
                "selections=" + selections +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitSelectionSet(this, context);
    }

    public static Builder newSelectionSet() {
        return new Builder();
    }

    public static Builder newSelectionSet(Collection<? extends Selection> selections) {
        return new Builder().selections(selections);
    }

    public SelectionSet transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {

        private List<Selection> selections = new ArrayList<>();
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(SelectionSet existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.selections = existing.getSelections();
            this.ignoredChars = existing.getIgnoredChars();
            this.additionalData = existing.getAdditionalData();
        }

        public Builder selections(Collection<? extends Selection> selections) {
            this.selections = new ArrayList<>(selections);
            return this;
        }

        public Builder selection(Selection selection) {
            this.selections.add(selection);
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

        public SelectionSet build() {
            return new SelectionSet(selections, sourceLocation, comments, ignoredChars, additionalData);
        }
    }
}
