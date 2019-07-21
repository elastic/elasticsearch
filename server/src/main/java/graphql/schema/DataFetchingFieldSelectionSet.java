package graphql.schema;

import graphql.execution.MergedSelectionSet;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This allows you to retrieve the selection set of fields that have been asked for when the
 * {@link DataFetcher} was invoked.
 *
 * For example imagine we are fetching the field 'user' in the following query
 *
 * <pre>
 * {@code
 *
 *  {
 *      user {
 *          name
 *          age
 *          weight
 *          friends {
 *              name
 *          }
 *      }
 *  }
 * }
 * </pre>
 *
 * The selection set in the case above consists of the fields "name, age, weight, friends and friends/name".
 *
 * You can use this selection set perhaps to "peek" ahead and decide that field values you might need
 * from the underlying data system.  Imagine a SQL system where this might represent the SQL 'projection'
 * of columns say.
 */
public interface DataFetchingFieldSelectionSet extends Supplier<MergedSelectionSet> {

    /**
     * @return a map of the fields that represent the selection set
     */
    @Override
    MergedSelectionSet get();

    /**
     * @return a map of the arguments for each field in the selection set
     */
    Map<String, Map<String, Object>> getArguments();

    /**
     * @return a map of the {@link graphql.schema.GraphQLFieldDefinition}s for each field in the selection set
     */
    Map<String, GraphQLFieldDefinition> getDefinitions();

    /**
     * This will return true if the field selection set matches a specified "glob" pattern matching ie
     * the glob pattern matching supported by {@link java.nio.file.FileSystem#getPathMatcher}.
     *
     * This will allow you to use '*', '**' and '?' as special matching characters such that "invoice/customer*" would
     * match an invoice field with child fields that start with 'customer'.
     *
     * @param fieldGlobPattern the glob pattern to match fields against
     *
     * @return true if the selection set contains these fields
     *
     * @see java.nio.file.FileSystem#getPathMatcher(String)
     */
    boolean contains(String fieldGlobPattern);

    /**
     * This will return true if the field selection set matches any of the specified "glob" pattern matches ie
     * the glob pattern matching supported by {@link java.nio.file.FileSystem#getPathMatcher}.
     *
     * This will allow you to use '*', '**' and '?' as special matching characters such that "invoice/customer*" would
     * match an invoice field with child fields that start with 'customer'.
     *
     * @param fieldGlobPattern  the glob pattern to match fields against
     * @param fieldGlobPatterns optionally more glob pattern to match fields against
     *
     * @return true if the selection set contains any of these these fields
     *
     * @see java.nio.file.FileSystem#getPathMatcher(String)
     */
    boolean containsAnyOf(String fieldGlobPattern, String... fieldGlobPatterns);

    /**
     * This will return true if the field selection set matches all of the specified "glob" pattern matches ie
     * the glob pattern matching supported by {@link java.nio.file.FileSystem#getPathMatcher}.
     *
     * This will allow you to use '*', '**' and '?' as special matching characters such that "invoice/customer*" would
     * match an invoice field with child fields that start with 'customer'.
     *
     * @param fieldGlobPattern  the glob pattern to match fields against
     * @param fieldGlobPatterns optionally more glob pattern to match fields against
     *
     * @return true if the selection set contains all of these these fields
     *
     * @see java.nio.file.FileSystem#getPathMatcher(String)
     */
    boolean containsAllOf(String fieldGlobPattern, String... fieldGlobPatterns);

    /**
     * This will return all selected fields.
     *
     * @return a list of all selected fields or empty list if none match
     */
    List<SelectedField> getFields();

    /**
     * This will return a list of selected fields that match a specified "glob" pattern matching ie
     * the glob pattern matching supported by {@link java.nio.file.FileSystem#getPathMatcher}.
     *
     * This will allow you to use '*', '**' and '?' as special matching characters such that "invoice/customer*" would
     * match an invoice field with child fields that start with 'customer'.
     *
     * @param fieldGlobPattern the glob pattern to match fields against
     *
     * @return a list of selected fields or empty list if none match
     */
    List<SelectedField> getFields(String fieldGlobPattern);

    /**
     * This will return a selected field using the fully qualified field name.
     *
     * @param fqFieldName the fully qualified name that is contained in the map from {@link #get()}
     *
     * @return a selected field or null if there is no matching field
     */
    SelectedField getField(String fqFieldName);


}
