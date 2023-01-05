package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.Stateless;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;

public class NewCommitNotificationAction extends ActionType<ActionResponse.Empty> {
    public static final NewCommitNotificationAction INSTANCE = new NewCommitNotificationAction();
    public static final String NAME = "internal:admin/" + Stateless.NAME + "/search/new/commit";

    private NewCommitNotificationAction() {
        super(NAME, in -> ActionResponse.Empty.INSTANCE);
    }
}
