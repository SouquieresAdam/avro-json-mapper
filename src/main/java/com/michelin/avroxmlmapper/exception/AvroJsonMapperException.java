package com.michelin.avroxmlmapper.exception;

/**
 * Exception thrown when an error occurs during the mapping process.
 */
public class AvroJsonMapperException extends RuntimeException {
    /**
     * Default constructor
     *
     * @param message The message
     * @param cause   The cause
     */
    public AvroJsonMapperException(String message, Throwable cause) {
        super(message, cause);
    }
}
