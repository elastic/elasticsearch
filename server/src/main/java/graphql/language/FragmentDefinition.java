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

/**
 * Provided to the DataFetcher, therefore public API
 */
@PublicApi
public class FragmentDefinition extends AbstractNode<FragmentDefinition> implements Definition<FragmentDefinition>, SelectionSetContainer<FragmentDefinition>, DirectivesContainer<FragmentDefinition>, NamedNode<FragmentDefinition> {

    private final String name;
    private final TypeName typeCondition;
    private final List<Directive> directives;
    private final SelectionSet selectionSet;

    public static final String CHILD_TYPE_CONDITION = "typeCondition";
    public static final String CHILD_DIRECTIVES = "directives";
    public static final String CHILD_SELECTION_SET = "selectionSet";

    @Internal
    protected FragmentDefinition(String name,
                                 TypeName typeCondition,
                                 List<Directive> directives,
                                 SelectionSet selectionSet,
                                 SourceLocation sourceLocation,
                                 List<Comment> comments,
                                 IgnoredChars ignoredChars,
                                 Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
        this.typeCondition = typeCondition;
        this.directives = directives;
        this.selectionSet = selectionSet;
    }

    @Override
    public String getName() {
        return name;
    }


    public TypeName getTypeCondition() {
        return typeCondition;
    }

    @Override
    public List<Directive> getDirectives() {
        return new ArrayList<>(directives);
    }


    @Override
    public SelectionSet getSelectionSet() {
        return selectionSet;
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.add(typeCondition);
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
    public FragmentDefinition withNewChildren(NodeChildrenContainer newChildren) {
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

        FragmentDefinition that = (FragmentDefinition) o;

        return NodeUtil.isEqualTo(this.name, that.name);
    }

    @Override
    public FragmentDefinition deepCopy() {
        return new FragmentDefinition(name,
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
        return "FragmentDefinition{" +
                "name='" + name + '\'' +
                ", typeCondition='" + typeCondition + '\'' +
                ", directives=" + directives +
                ", selectionSet=" + selectionSet +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor nodeVisitor) {
        return nodeVisitor.visitFragmentDefinition(this, context);
    }

    public static Builder newFragmentDefinition() {
        return new Builder();
    }

    public FragmentDefinition transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private String name;
        private TypeName typeCondition;
        private List<Directive> directives = new ArrayList<>();
        private SelectionSet selectionSet;
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(FragmentDefinition existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
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

        public Builder name(String name) {
            this.name = name;
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


        public FragmentDefinition build() {
            return new FragmentDefinition(name, typeCondition, directives, selectionSet, sourceLocation, comments, ignoredChars, additionalData);
        }
    }
}
