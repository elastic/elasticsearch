#!/bin/sh -e

case "\$1" in
    install|upgrade)
        <% commands.each {command -> %>
        <%= command %>
        <% } %>
        ;;
esac
