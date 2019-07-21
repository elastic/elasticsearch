package graphql.validation.rules;

import graphql.Directives;
import graphql.Internal;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.validation.AbstractRule;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationError;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

@Internal
public class DeferredMustBeOnAllFields extends AbstractRule {

    private final Map<List<String>, List<Field>> fieldsByPath = new LinkedHashMap<>();

    public DeferredMustBeOnAllFields(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }

    @Override
    public void documentFinished(Document document) {
        fieldsByPath.forEach((path, allFieldsOnPath) -> {
            Map<String, List<Field>> fieldsByName = allFieldsOnPath.stream()
                    .collect(groupingBy(Field::getName));
            fieldsByName.forEach((fieldName, fields) -> {
                if (fields.size() > 1) {
                    if (!allFieldsHaveDeferredDirectiveIfAnyOneDoes(fields)) {
                        recordBadDeferredFields(path, fieldName, fields);
                    }
                }
            });
        });
    }

    private boolean allFieldsHaveDeferredDirectiveIfAnyOneDoes(List<Field> fields) {
        long count = fields.stream()
                .filter(fld -> fld.getDirective(Directives.DeferDirective.getName()) != null)
                .count();
        if (count == 0) {
            return true;
        } else {
            return count == fields.size();
        }
    }

    private void recordBadDeferredFields(List<String> path, String fieldName, List<Field> fields) {
        String message = String.format("If any of the multiple declarations of a field within the query (via fragments and field selections) contain the @defer directive, then all of them have to contain the @defer directive - field '%s' at '%s'", fieldName, path);
        getValidationErrorCollector().addError(new ValidationError(ValidationErrorType.DeferMustBeOnAllFields, fields.get(0).getSourceLocation(), message, path));
    }

    @Override
    public void checkSelectionSet(SelectionSet selectionSet) {
        List<String> queryPath = getValidationContext().getQueryPath();
        addFields(queryPath, selectionSet);
    }

    private void addFields(List<String> path, SelectionSet selectionSet) {
        if (path == null) {
            path = Collections.emptyList();
        }
        for (Selection selection : selectionSet.getSelections()) {
            if (selection instanceof Field) {
                List<Field> fields = fieldsByPath.getOrDefault(path, new ArrayList<>());
                fields.add((Field) selection);
                fieldsByPath.put(path, fields);
            }
            if (selection instanceof InlineFragment) {
                addFields(path, ((InlineFragment) selection).getSelectionSet());
            }
            if (selection instanceof FragmentSpread) {
                FragmentSpread fragmentSpread = (FragmentSpread) selection;
                FragmentDefinition fragmentDefinition = getValidationContext().getFragment(fragmentSpread.getName());
                if (fragmentDefinition != null) {
                    addFields(path, fragmentDefinition.getSelectionSet());
                }
            }
        }
    }
}
