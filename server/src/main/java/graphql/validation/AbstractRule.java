package graphql.validation;


import java.util.ArrayList;
import java.util.List;

import graphql.Internal;
import graphql.language.Argument;
import graphql.language.Directive;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.Node;
import graphql.language.OperationDefinition;
import graphql.language.SelectionSet;
import graphql.language.SourceLocation;
import graphql.language.TypeName;
import graphql.language.VariableDefinition;
import graphql.language.VariableReference;

@Internal
public class AbstractRule {

    private final ValidationContext validationContext;
    private final ValidationErrorCollector validationErrorCollector;


    private boolean visitFragmentSpreads;

    private ValidationUtil validationUtil = new ValidationUtil();

    public AbstractRule(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        this.validationContext = validationContext;
        this.validationErrorCollector = validationErrorCollector;
    }

    public boolean isVisitFragmentSpreads() {
        return visitFragmentSpreads;
    }

    public void setVisitFragmentSpreads(boolean visitFragmentSpreads) {
        this.visitFragmentSpreads = visitFragmentSpreads;
    }


    public ValidationUtil getValidationUtil() {
        return validationUtil;
    }

    public void setValidationUtil(ValidationUtil validationUtil) {
        this.validationUtil = validationUtil;
    }

    public void addError(ValidationErrorType validationErrorType, List<? extends Node> locations, String description) {
        List<SourceLocation> locationList = new ArrayList<>();
        for (Node node : locations) {
            locationList.add(node.getSourceLocation());
        }
        validationErrorCollector.addError(new ValidationError(validationErrorType, locationList, description, getQueryPath()));
    }

    public void addError(ValidationErrorType validationErrorType, SourceLocation location, String description) {
        validationErrorCollector.addError(new ValidationError(validationErrorType, location, description, getQueryPath()));
    }

    public List<ValidationError> getErrors() {
        return validationErrorCollector.getErrors();
    }


    public ValidationContext getValidationContext() {
        return validationContext;
    }

    public ValidationErrorCollector getValidationErrorCollector() {
        return validationErrorCollector;
    }

    protected List<String> getQueryPath() {
        return validationContext.getQueryPath();
    }

    public void checkDocument(Document document) {

    }

    public void checkArgument(Argument argument) {

    }

    public void checkTypeName(TypeName typeName) {

    }

    public void checkVariableDefinition(VariableDefinition variableDefinition) {

    }

    public void checkField(Field field) {

    }

    public void checkInlineFragment(InlineFragment inlineFragment) {

    }

    public void checkDirective(Directive directive, List<Node> ancestors) {

    }

    public void checkFragmentSpread(FragmentSpread fragmentSpread) {

    }

    public void checkFragmentDefinition(FragmentDefinition fragmentDefinition) {

    }

    public void checkOperationDefinition(OperationDefinition operationDefinition) {

    }

    public void leaveOperationDefinition(OperationDefinition operationDefinition) {

    }

    public void checkSelectionSet(SelectionSet selectionSet) {

    }

    public void leaveSelectionSet(SelectionSet selectionSet) {

    }

    public void checkVariable(VariableReference variableReference) {

    }

    public void documentFinished(Document document) {

    }

    @Override
    public String toString() {
        return "Rule{" + validationContext + "}";
    }
}
