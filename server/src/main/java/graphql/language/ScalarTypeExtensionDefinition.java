package graphql.language;

import graphql.Internal;
import graphql.PublicApi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;

@PublicApi
public class ScalarTypeExtensionDefinition extends ScalarTypeDefinition {

    @Internal
    protected ScalarTypeExtensionDefinition(String name,
                                            List<Directive> directives,
                                            Description description,
                                            SourceLocation sourceLocation,
                                            List<Comment> comments,
                                            IgnoredChars ignoredChars, Map<String, String> additionalData) {
        super(name, directives, description, sourceLocation, comments, ignoredChars, additionalData);
    }

    @Override
    public ScalarTypeExtensionDefinition deepCopy() {
        return new ScalarTypeExtensionDefinition(getName(), deepCopy(getDirectives()), getDescription(), getSourceLocation(), getComments(), getIgnoredChars(), getAdditionalData());
    }

    @Override
    public String toString() {
        return "ScalarTypeExtensionDefinition{" +
                "name='" + getName() + '\'' +
                ", directives=" + getDirectives() +
                '}';

    }

    public static Builder newScalarTypeExtensionDefinition() {
        return new Builder();
    }

    public ScalarTypeExtensionDefinition transformExtension(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private String name;
        private Description description;
        private List<Directive> directives = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }


        private Builder(ScalarTypeExtensionDefinition existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.description = existing.getDescription();
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

        public Builder description(Description description) {
            this.description = description;
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


        public ScalarTypeExtensionDefinition build() {
            return new ScalarTypeExtensionDefinition(name,
                    directives,
                    description,
                    sourceLocation,
                    comments,
                    ignoredChars,
                    additionalData);
        }
    }
}
