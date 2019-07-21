package graphql.schema;

import graphql.Internal;
import graphql.execution.ExecutionContext;
import graphql.execution.FieldCollector;
import graphql.execution.FieldCollectorParameters;
import graphql.execution.MergedField;
import graphql.execution.MergedSelectionSet;
import graphql.execution.ValuesResolver;
import graphql.introspection.Introspection;
import graphql.language.Field;
import graphql.language.FragmentDefinition;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static graphql.Assert.assertNotNull;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

@Internal
public class DataFetchingFieldSelectionSetImpl implements DataFetchingFieldSelectionSet {

    private final static DataFetchingFieldSelectionSet NOOP = new DataFetchingFieldSelectionSet() {
        @Override
        public MergedSelectionSet get() {
            return MergedSelectionSet.newMergedSelectionSet().build();
        }

        @Override
        public Map<String, Map<String, Object>> getArguments() {
            return emptyMap();
        }

        @Override
        public Map<String, GraphQLFieldDefinition> getDefinitions() {
            return emptyMap();
        }

        @Override
        public boolean contains(String fieldGlobPattern) {
            return false;
        }

        @Override
        public boolean containsAnyOf(String fieldGlobPattern, String... fieldGlobPatterns) {
            return false;
        }

        @Override
        public boolean containsAllOf(String fieldGlobPattern, String... fieldGlobPatterns) {
            return false;
        }

        @Override
        public SelectedField getField(String fieldName) {
            return null;
        }

        @Override
        public List<SelectedField> getFields() {
            return emptyList();
        }

        @Override
        public List<SelectedField> getFields(String fieldGlobPattern) {
            return emptyList();
        }
    };

    public static DataFetchingFieldSelectionSet newCollector(ExecutionContext executionContext, GraphQLType fieldType, MergedField mergedField) {
        GraphQLType unwrappedType = GraphQLTypeUtil.unwrapAll(fieldType);
        if (unwrappedType instanceof GraphQLFieldsContainer) {
            return new DataFetchingFieldSelectionSetImpl(executionContext, (GraphQLFieldsContainer) unwrappedType, mergedField);
        } else {
            // we can only collect fields on object types and interfaces.  Scalars, Unions etc... cant be done.
            return NOOP;
        }
    }

    private static GraphQLObjectType asObjectTypeOrNull(GraphQLType unwrappedType) {
        return unwrappedType instanceof GraphQLObjectType ? (GraphQLObjectType) unwrappedType : null;
    }

    private final FieldCollector fieldCollector = new FieldCollector();
    private final ValuesResolver valuesResolver = new ValuesResolver();

    private final MergedField parentFields;
    private final GraphQLSchema graphQLSchema;
    private final GraphQLFieldsContainer parentFieldType;
    private final Map<String, Object> variables;
    private final Map<String, FragmentDefinition> fragmentsByName;

    private Map<String, MergedField> selectionSetFields;
    private Map<String, GraphQLFieldDefinition> selectionSetFieldDefinitions;
    private Map<String, Map<String, Object>> selectionSetFieldArgs;
    private Set<String> flattenedFields;

    private DataFetchingFieldSelectionSetImpl(ExecutionContext executionContext, GraphQLFieldsContainer parentFieldType, MergedField parentFields) {
        this(parentFields, parentFieldType, executionContext.getGraphQLSchema(), executionContext.getVariables(), executionContext.getFragmentsByName());
    }

    public DataFetchingFieldSelectionSetImpl(MergedField parentFields, GraphQLFieldsContainer parentFieldType, GraphQLSchema graphQLSchema, Map<String, Object> variables, Map<String, FragmentDefinition> fragmentsByName) {
        this.parentFields = parentFields;
        this.graphQLSchema = graphQLSchema;
        this.parentFieldType = parentFieldType;
        this.variables = variables;
        this.fragmentsByName = fragmentsByName;
    }

    @Override
    public MergedSelectionSet get() {
        // by having a .get() method we get lazy evaluation.
        computeValuesLazily();
        return MergedSelectionSet.newMergedSelectionSet().subFields(selectionSetFields).build();
    }

    @Override
    public Map<String, Map<String, Object>> getArguments() {
        computeValuesLazily();
        return selectionSetFieldArgs;
    }

    @Override
    public Map<String, GraphQLFieldDefinition> getDefinitions() {
        computeValuesLazily();
        return selectionSetFieldDefinitions;
    }

    @Override
    public boolean contains(String fieldGlobPattern) {
        if (fieldGlobPattern == null || fieldGlobPattern.isEmpty()) {
            return false;
        }
        computeValuesLazily();
        PathMatcher globMatcher = globMatcher(fieldGlobPattern);
        for (String flattenedField : flattenedFields) {
            Path path = Paths.get(flattenedField);
            if (globMatcher.matches(path)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsAnyOf(String fieldGlobPattern, String... fieldGlobPatterns) {
        assertNotNull(fieldGlobPattern);
        assertNotNull(fieldGlobPatterns);
        for (String globPattern : mkIterable(fieldGlobPattern, fieldGlobPatterns)) {
            if (contains(globPattern)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsAllOf(String fieldGlobPattern, String... fieldGlobPatterns) {
        assertNotNull(fieldGlobPattern);
        assertNotNull(fieldGlobPatterns);
        for (String globPattern : mkIterable(fieldGlobPattern, fieldGlobPatterns)) {
            if (!contains(globPattern)) {
                return false;
            }
        }
        return true;
    }

    private List<String> mkIterable(String fieldGlobPattern, String[] fieldGlobPatterns) {
        List<String> l = new ArrayList<>();
        l.add(fieldGlobPattern);
        Collections.addAll(l, fieldGlobPatterns);
        return l;
    }

    @Override
    public SelectedField getField(String fqFieldName) {
        computeValuesLazily();

        MergedField fields = selectionSetFields.get(fqFieldName);
        if (fields == null) {
            return null;
        }
        GraphQLFieldDefinition fieldDefinition = selectionSetFieldDefinitions.get(fqFieldName);
        Map<String, Object> arguments = selectionSetFieldArgs.get(fqFieldName);
        arguments = arguments == null ? emptyMap() : arguments;
        return new SelectedFieldImpl(fqFieldName, fields, fieldDefinition, arguments);
    }

    @Override
    public List<SelectedField> getFields(String fieldGlobPattern) {
        if (fieldGlobPattern == null || fieldGlobPattern.isEmpty()) {
            return emptyList();
        }
        computeValuesLazily();

        List<String> targetNames = new ArrayList<>();
        PathMatcher globMatcher = globMatcher(fieldGlobPattern);
        for (String flattenedField : flattenedFields) {
            Path path = Paths.get(flattenedField);
            if (globMatcher.matches(path)) {
                targetNames.add(flattenedField);
            }
        }
        return targetNames.stream()
                .map(this::getField)
                .collect(Collectors.toList());
    }

    @Override
    public List<SelectedField> getFields() {
        computeValuesLazily();

        return flattenedFields.stream()
                .map(this::getField)
                .collect(Collectors.toList());
    }

    private class SelectedFieldImpl implements SelectedField {
        private final String qualifiedName;
        private final String name;
        private final GraphQLFieldDefinition fieldDefinition;
        private final DataFetchingFieldSelectionSet selectionSet;
        private final Map<String, Object> arguments;

        private SelectedFieldImpl(String qualifiedName, MergedField parentFields, GraphQLFieldDefinition fieldDefinition, Map<String, Object> arguments) {
            this.qualifiedName = qualifiedName;
            this.name = parentFields.getName();
            this.fieldDefinition = fieldDefinition;
            this.arguments = arguments;
            GraphQLType unwrappedType = GraphQLTypeUtil.unwrapAll(fieldDefinition.getType());
            if (unwrappedType instanceof GraphQLFieldsContainer) {
                this.selectionSet = new DataFetchingFieldSelectionSetImpl(parentFields, (GraphQLFieldsContainer) unwrappedType, graphQLSchema, variables, fragmentsByName);
            } else {
                this.selectionSet = NOOP;
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getQualifiedName() {
            return qualifiedName;
        }

        @Override
        public GraphQLFieldDefinition getFieldDefinition() {
            return fieldDefinition;
        }

        @Override
        public Map<String, Object> getArguments() {
            return arguments;
        }

        @Override
        public DataFetchingFieldSelectionSet getSelectionSet() {
            return selectionSet;
        }
    }

    private PathMatcher globMatcher(String fieldGlobPattern) {
        return FileSystems.getDefault().getPathMatcher("glob:" + fieldGlobPattern);
    }

    private void computeValuesLazily() {
        synchronized (this) {
            if (selectionSetFields != null) {
                return;
            }

            selectionSetFields = new LinkedHashMap<>();
            selectionSetFieldDefinitions = new LinkedHashMap<>();
            selectionSetFieldArgs = new LinkedHashMap<>();
            flattenedFields = new LinkedHashSet<>();

            traverseFields(parentFields, parentFieldType, "");
        }
    }

    private final static String SEP = "/";


    private void traverseFields(MergedField fieldList, GraphQLFieldsContainer parentFieldType, String fieldPrefix) {

        FieldCollectorParameters parameters = FieldCollectorParameters.newParameters()
                .schema(graphQLSchema)
                .objectType(asObjectTypeOrNull(parentFieldType))
                .fragments(fragmentsByName)
                .variables(variables)
                .build();

        MergedSelectionSet collectedFields = fieldCollector.collectFields(parameters, fieldList);
        for (Map.Entry<String, MergedField> entry : collectedFields.getSubFields().entrySet()) {
            String fieldName = mkFieldName(fieldPrefix, entry.getKey());
            MergedField collectedFieldList = entry.getValue();
            selectionSetFields.put(fieldName, collectedFieldList);

            Field field = collectedFieldList.getSingleField();
            GraphQLFieldDefinition fieldDef = Introspection.getFieldDef(graphQLSchema, parentFieldType, field.getName());
            GraphQLType unwrappedType = GraphQLTypeUtil.unwrapAll(fieldDef.getType());
            Map<String, Object> argumentValues = valuesResolver.getArgumentValues(fieldDef.getArguments(), field.getArguments(), variables);

            selectionSetFieldArgs.put(fieldName, argumentValues);
            selectionSetFieldDefinitions.put(fieldName, fieldDef);
            flattenedFields.add(fieldName);

            if (unwrappedType instanceof GraphQLFieldsContainer) {
                traverseFields(collectedFieldList, (GraphQLFieldsContainer) unwrappedType, fieldName);
            }
        }
    }

    private String mkFieldName(String fieldPrefix, String fieldName) {
        return (!fieldPrefix.isEmpty() ? fieldPrefix + SEP : "") + fieldName;
    }
}
