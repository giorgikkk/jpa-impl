package com.example.exceptions;

public class ConstructorNotFoundException extends Exception {
    public ConstructorNotFoundException(String message) {
        super(message);
    }
}
