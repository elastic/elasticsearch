package graphql.analysis;

import graphql.PublicApi;
import graphql.execution.AbortExecutionException;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.SimpleInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationValidationParameters;
import graphql.validation.ValidationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static graphql.Assert.assertNotNull;
import static graphql.execution.instrumentation.SimpleInstrumentationContext.whenCompleted;
import static java.util.Optional.ofNullable;

/**
 * Prevents execution if the query complexity is greater than the specified maxComplexity.
 *
 * Use the {@code Function<QueryComplexityInfo, Boolean>} parameter to supply a function to perform a custom action when the max complexity
 * is exceeded. If the function returns {@code true} a {@link AbortExecutionException} is thrown.
 */
@PublicApi
public class MaxQueryComplexityInstrumentation extends SimpleInstrumentation {

    private static final Logger log = LoggerFactory.getLogger(MaxQueryComplexityInstrumentation.class);

    private final int maxComplexity;
    private final FieldComplexityCalculator fieldComplexityCalculator;
    private final Function<QueryComplexityInfo, Boolean> maxQueryComplexityExceededFunction;

    /**
     * new Instrumentation with default complexity calculator which is `1 + childComplexity`
     *
     * @param maxComplexity max allowed complexity, otherwise execution will be aborted
     */
    public MaxQueryComplexityInstrumentation(int maxComplexity) {
        this(maxComplexity, (queryComplexityInfo) -> true);
    }

    /**
     * new Instrumentation with default complexity calculator which is `1 + childComplexity`
     *
     * @param maxComplexity                      max allowed complexity, otherwise execution will be aborted
     * @param maxQueryComplexityExceededFunction the function to perform when the max complexity is exceeded
     */
    public MaxQueryComplexityInstrumentation(int maxComplexity, Function<QueryComplexityInfo, Boolean> maxQueryComplexityExceededFunction) {
        this(maxComplexity, (env, childComplexity) -> 1 + childComplexity, maxQueryComplexityExceededFunction);
    }

    /**
     * new Instrumentation with custom complexity calculator
     *
     * @param maxComplexity             max allowed complexity, otherwise execution will be aborted
     * @param fieldComplexityCalculator custom complexity calculator
     */
    public MaxQueryComplexityInstrumentation(int maxComplexity, FieldComplexityCalculator fieldComplexityCalculator) {
        this(maxComplexity, fieldComplexityCalculator, (queryComplexityInfo) -> true);
    }

    /**
     * new Instrumentation with custom complexity calculator
     *
     * @param maxComplexity                      max allowed complexity, otherwise execution will be aborted
     * @param fieldComplexityCalculator          custom complexity calculator
     * @param maxQueryComplexityExceededFunction the function to perform when the max complexity is exceeded
     */
    public MaxQueryComplexityInstrumentation(int maxComplexity, FieldComplexityCalculator fieldComplexityCalculator,
                                             Function<QueryComplexityInfo, Boolean> maxQueryComplexityExceededFunction) {
        this.maxComplexity = maxComplexity;
        this.fieldComplexityCalculator = assertNotNull(fieldComplexityCalculator, "calculator can't be null");
        this.maxQueryComplexityExceededFunction = maxQueryComplexityExceededFunction;
    }

    @Override
    public InstrumentationContext<List<ValidationError>> beginValidation(InstrumentationValidationParameters parameters) {
        return whenCompleted((errors, throwable) -> {
            if ((errors != null && errors.size() > 0) || throwable != null) {
                return;
            }
            QueryTraverser queryTraverser = newQueryTraverser(parameters);

            Map<QueryVisitorFieldEnvironment, Integer> valuesByParent = new LinkedHashMap<>();
            queryTraverser.visitPostOrder(new QueryVisitorStub() {
                @Override
                public void visitField(QueryVisitorFieldEnvironment env) {
                    int childsComplexity = valuesByParent.getOrDefault(env, 0);
                    int value = calculateComplexity(env, childsComplexity);

                    valuesByParent.compute(env.getParentEnvironment(), (key, oldValue) ->
                            ofNullable(oldValue).orElse(0) + value
                    );
                }
            });
            int totalComplexity = valuesByParent.getOrDefault(null, 0);
            log.debug("Query complexity: {}", totalComplexity);
            if (totalComplexity > maxComplexity) {
                QueryComplexityInfo queryComplexityInfo = QueryComplexityInfo.newQueryComplexityInfo()
                        .complexity(totalComplexity)
                        .build();
                boolean throwAbortException = maxQueryComplexityExceededFunction.apply(queryComplexityInfo);
                if (throwAbortException) {
                    throw mkAbortException(totalComplexity, maxComplexity);
                }
            }
        });
    }

    /**
     * Called to generate your own error message or custom exception class
     *
     * @param totalComplexity the complexity of the query
     * @param maxComplexity   the maximum complexity allowed
     *
     * @return a instance of AbortExecutionException
     */
    protected AbortExecutionException mkAbortException(int totalComplexity, int maxComplexity) {
        return new AbortExecutionException("maximum query complexity exceeded " + totalComplexity + " > " + maxComplexity);
    }

    QueryTraverser newQueryTraverser(InstrumentationValidationParameters parameters) {
        return QueryTraverser.newQueryTraverser()
                .schema(parameters.getSchema())
                .document(parameters.getDocument())
                .operationName(parameters.getOperation())
                .variables(parameters.getVariables())
                .build();
    }

    private int calculateComplexity(QueryVisitorFieldEnvironment queryVisitorFieldEnvironment, int childsComplexity) {
        if (queryVisitorFieldEnvironment.isTypeNameIntrospectionField()) {
            return 0;
        }
        FieldComplexityEnvironment fieldComplexityEnvironment = convertEnv(queryVisitorFieldEnvironment);
        return fieldComplexityCalculator.calculate(fieldComplexityEnvironment, childsComplexity);
    }

    private FieldComplexityEnvironment convertEnv(QueryVisitorFieldEnvironment queryVisitorFieldEnvironment) {
        FieldComplexityEnvironment parentEnv = null;
        if (queryVisitorFieldEnvironment.getParentEnvironment() != null) {
            parentEnv = convertEnv(queryVisitorFieldEnvironment.getParentEnvironment());
        }
        return new FieldComplexityEnvironment(
                queryVisitorFieldEnvironment.getField(),
                queryVisitorFieldEnvironment.getFieldDefinition(),
                queryVisitorFieldEnvironment.getFieldsContainer(),
                queryVisitorFieldEnvironment.getArguments(),
                parentEnv
        );
    }


}
