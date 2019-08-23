package org.elasticsearch.common.settings;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class HasPasswordKeyStoreCommandTests extends KeyStoreCommandTestCase {
    @Override
    protected Command newCommand() {
        return new HasPasswordKeyStoreCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    public void testFailsWithNoKeystore() throws Exception {
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(e.getMessage(), HasPasswordKeyStoreCommand.NO_PASSWORD_EXIT_CODE, e.exitCode);
        assertThat(e.getMessage(), containsString("Elasticsearch keystore not found"));
    }

    public void testFailsWhenKeystoreLacksPassword() throws Exception {
        createKeystore("");
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(e.getMessage(), HasPasswordKeyStoreCommand.NO_PASSWORD_EXIT_CODE, e.exitCode);
        assertThat(e.getMessage(), containsString("Keystore is password protected"));
    }

    public void testSucceedsWhenKeystoreHasPassword() throws Exception {
        createKeystore("password");
        String output = execute();
        assertThat(output, containsString("Keystore is password-protected"));
    }

    public void testSilentSucceedsWhenKeystoreHasPassword() throws Exception {
        createKeystore("password");
        String output = execute("--silent");
        assertEquals("", output);
    }
}
