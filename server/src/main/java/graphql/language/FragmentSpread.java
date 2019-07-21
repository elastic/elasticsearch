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
public class FragmentSpread extends AbstractNode<FragmentSpread> implements Selection<FragmentSpread>, DirectivesContainer<FragmentSpread>, NamedNode<FragmentSpread> {

    private final String name;
    private final List<Directive> directives;

    public static final String CHILD_DIRECTIVES = "directives";

    @Internal
    protected FragmentSpread(String name, List<Directive> directives, SourceLocation sourceLocation, List<Comment> comments, IgnoredChars ignoredChars, Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
        this.directives = new ArrayList<>(directives);
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name of the fragment
     */
    public FragmentSpread(String name) {
        this(name, new ArrayList<>(), null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Directive> getDirectives() {
        return new ArrayList<>(directives);
    }

    @Override
    public boolean isEqualTo(Node o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FragmentSpread that = (FragmentSpread) o;

        return NodeUtil.isEqualTo(this.name, that.name);
    }


    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.addAll(directives);
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .children(CHILD_DIRECTIVES, directives)
                .build();
    }

    @Override
    public FragmentSpread withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .directives(newChildren.getChildren(CHILD_DIRECTIVES))
        );
    }

    @Override
    public FragmentSpread deepCopy() {
        return new FragmentSpread(name, deepCopy(directives), getSourceLocation(), getComments(), getIgnoredChars(), getAdditionalData());
    }

    @Override
    public String toString() {
        return "FragmentSpread{" +
                "name='" + name + '\'' +
                ", directives=" + directives +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitFragmentSpread(this, context);
    }

    public static Builder newFragmentSpread() {
        return new Builder();
    }

    public static Builder newFragmentSpread(String name) {
        return new Builder().name(name);
    }


    public FragmentSpread transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private String name;
        private List<Directive> directives = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(FragmentSpread existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.directives = existing.getDirectives();
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

        public Builder directives(List<Directive> directives) {
            this.directives = directives;
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


        public FragmentSpread build() {
            return new FragmentSpread(name, directives, sourceLocation, comments, ignoredChars, additionalData);
        }
    }
}
