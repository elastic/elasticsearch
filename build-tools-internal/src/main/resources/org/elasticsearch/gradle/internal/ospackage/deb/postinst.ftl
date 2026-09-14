#!/bin/sh -e

ec() {
    echo "\$@" >&2
    "\$@"
}

case "\$1" in
    configure)
        <% dirs.each{ dir -> %>
            ec <%= dir['install'] %>
        <% } %>

        <% commands.each {command -> %>
        <%= command %>
        <% } %>
        ;;
esac
