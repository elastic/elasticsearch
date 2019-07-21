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
import static graphql.language.NodeUtil.directivesByName;
import static java.util.Collections.emptyMap;

@PublicApi
public class InlineFragment extends AbstractNode<InlineFragment> implements Selection<InlineFragment>, SelectionSetContainer<InlineFragment>, DirectivesContainer<InlineFragment> {
    private final TypeName typeCondition;
    private final List<Directive> directives;
    private final SelectionSet selectionSet;

    public static final String CHILD_TYPE_CONDITION = "typeCondition";
    public static final String CHILD_DIRECTIVES = "directives";
    public static final String CHILD_SELECTION_SET = "selectionSet";

    @Internal
    protected InlineFragment(TypeName typeCondition,
                             List<Directive> directives,
                             SelectionSet selectionSet,
                             SourceLocation sourceLocation,
                             List<Comment> comments,
                             IgnoredChars ignoredChars,
                             Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.typeCondition = typeCondition;
        this.directives = directives;
        this.selectionSet = selectionSet;
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param typeCondition the type condition of the inline fragment
     */
    public InlineFragment(TypeName typeCondition) {
        this(typeCondition, new ArrayList<>(), null, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param typeCondition the type condition of the inline fragment
     * @param selectionSet  of the inline fragment
     */
    public InlineFragment(TypeName typeCondition, SelectionSet selectionSet) {
        this(typeCondition, new ArrayList<>(), selectionSet, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    public TypeName getTypeCondition() {
        return typeCondition;
    }

    public List<Directive> getDirectives() {
        return new ArrayList<>(directives);
    }

    public Map<String, Directive> getDirectivesByName() {
        return directivesByName(directives);
    }

    public Directive getDirective(String directiveName) {
        return getDirectivesByName().get(directiveName);
    }

    @Override
    public SelectionSet getSelectionSet() {
        return selectionSet;
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        if (typeCondition != null) {
            result.add(typeCondition);
        }
        result.addAll(directives);
        result.add(selectionSet);
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .child(CHILD_TYPE_CONDITION, typeCondition)
                .children(CHILD_DIRECTIVES, directives)
                .child(CHILD_SELECTION_SET, selectionSet)
                .build();
    }

    @Override
    public InlineFragment withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .typeCondition(newChildren.getChildOrNull(CHILD_TYPE_CONDITION))
                .directives(newChildren.getChildren(CHILD_DIRECTIVES))
                .selectionSet(newChildren.getChildOrNull(CHILD_SELECTION_SET))
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
    public InlineFragment deepCopy() {
        return new InlineFragment(
                deepCopy(typeCondition),
                deepCopy(directives),
                deepCopy(selectionSet),
                getSourceLocation(),
                getComments(),
                getIgnoredChars(),
                getAdditionalData());
    }

    @Override
    public String toString() {
        return "InlineFragment{" +
                "typeCondition='" + typeCondition + '\'' +
                ", directives=" + directives +
                ", selectionSet=" + selectionSet +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitInlineFragment(this, context);
    }

    public static Builder newInlineFragment() {
        return new Builder();
    }

    public InlineFragment transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private TypeName typeCondition;
        private List<Directive> directives = new ArrayList<>();
        private SelectionSet selectionSet;
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }


        private Builder(InlineFragment existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.typeCondition = existing.getTypeCondition();
            this.directives = existing.getDirectives();
            this.selectionSet = existing.getSelectionSet();
            this.ignoredChars = existing.getIgnoredChars();
            this.additionalData = existing.getAdditionalData();
        }


        public Builder sourceLocation(SourceLocation sourceLocation) {
            this.sourceLocation = sourceLocation;
            return this;
        }

        public Builder comments(List<Comment> comments) {
            this.comments = comments;
            return this;
        }

        public Builder typeCondition(TypeName typeCondition) {
            this.typeCondition = typeCondition;
            return this;
        }

        public Builder directives(List<Directive> directives) {
            this.directives = directives;
            return this;
        }

        public Builder selectionSet(SelectionSet selectionSet) {
            this.selectionSet = selectionSet;
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


        public InlineFragment build() {
            return new InlineFragment(typeCondition, directives, selectionSet, sourceLocation, comments, ignoredChars, additionalData);
        }
    }
}
