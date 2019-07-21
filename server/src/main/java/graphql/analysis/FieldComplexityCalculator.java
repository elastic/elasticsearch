package graphql.analysis;

import graphql.PublicApi;

/**
 * Used to calculate the complexity of a field. Used by {@link MaxQueryComplexityInstrumentation}.
 */
@PublicApi
@FunctionalInterface
public interface FieldComplexityCalculator {

    /**
     * Calculates the complexity of a field
     *
     * @param environment     several information about the current field
     * @param childComplexity the sum of all child complexity scores
     *
     * @return the calculated complexity
     */
    int calculate(FieldComplexityEnvironment environment, int childComplexity);

}
