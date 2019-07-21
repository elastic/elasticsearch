package graphql.execution.nextgen.result;

import graphql.GraphQLError;
import graphql.Internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

@Internal
public class ResolvedValue {

    private final Object completedValue;
    private final Object localContext;
    private final boolean nullValue;
    private final List<GraphQLError> errors;

    private ResolvedValue(Builder builder) {
        this.completedValue = builder.completedValue;
        this.localContext = builder.localContext;
        this.nullValue = builder.nullValue;
        this.errors = builder.errors;
    }

    public Object getCompletedValue() {
        return completedValue;
    }

    public Object getLocalContext() {
        return localContext;
    }

    public boolean isNullValue() {
        return nullValue;
    }

    public List<GraphQLError> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    public static Builder newResolvedValue() {
        return new Builder();
    }


    public ResolvedValue transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }


    public static class Builder {
        private Object completedValue;
        private Object localContext;
        private boolean nullValue;
        private List<GraphQLError> errors = Collections.emptyList();

        private Builder() {

        }

        private Builder(ResolvedValue existing) {
            this.completedValue = existing.completedValue;
            this.localContext = existing.localContext;
            this.nullValue = existing.nullValue;
            this.errors = existing.errors;
        }

        public Builder completedValue(Object completedValue) {
            this.completedValue = completedValue;
            return this;
        }

        public Builder localContext(Object localContext) {
            this.localContext = localContext;
            return this;
        }

        public Builder nullValue(boolean nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder errors(List<GraphQLError> errors) {
            this.errors = new ArrayList<>(errors);
            return this;
        }

        public ResolvedValue build() {
            return new ResolvedValue(this);
        }
    }

}
