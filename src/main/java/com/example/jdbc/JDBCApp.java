package com.example.jdbc;

import com.example.annotations.Column;
import com.example.annotations.Table;
import com.example.db.DBConnection;
import com.example.exceptions.IdNotFoundException;
import com.example.annotations.Id;
import com.example.exceptions.ConstructorNotFoundException;
import com.example.exceptions.InvalidAnnotatedFieldException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public final class JDBCApp {

    public static boolean insert(Object o) {
        Class<?> clazz = o.getClass();
        Table table = clazz.getAnnotation(Table.class);

        Field[] fields = clazz.getDeclaredFields();
        String columnNames = getColumnNames(fields);
        String fieldValues = getFieldValues(fields, o);

        String name = table.scheme() + "." + table.name();

        String query = String.format("INSERT INTO %s (%s)\n" +
                        "VALUES(%s);",
                name, columnNames, fieldValues);

        return insertObject(query);
    }


    private static boolean insertObject(String query) {
        try (Connection conn = DBConnection.INSTANCE.getConnection()) {
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            int insertedRowsNum = preparedStatement.executeUpdate();

            return insertedRowsNum == 1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static String getFieldValues(Field[] fields, Object o) {
        List<String> fieldValues = Stream.of(fields).
                filter(f -> f.isAnnotationPresent(Column.class))
                .map(f -> {
                    try {
                        f.setAccessible(true);
                        return f.get(o) instanceof Number ? ("" + f.get(o)) : ("'" + f.get(o) + "'");
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());

        return String.join(",", fieldValues);
    }


    private static String getColumnNames(Field[] fields) {
        List<String> columnNames = Stream.of(fields).
                filter(f -> f.isAnnotationPresent(Column.class))
                .map(f -> f.getAnnotation(Column.class).name())
                .collect(Collectors.toList());

        return String.join(",", columnNames);
    }


    public static boolean update(Object o) {
        Class<?> clazz = o.getClass();
        Table table = clazz.getAnnotation(Table.class);

        String name = table.scheme() + "." + table.name();
        Field[] fields = clazz.getDeclaredFields();

        String columnsToUpdate = getColumnsToUpdate(fields, o);
        String idsToUpdate = getIdsToUpdate(fields, o);

        String query = String.format("UPDATE %s\n" +
                        "SET %s\n" +
                        "WHERE %s;",
                name, columnsToUpdate, idsToUpdate);

        return updateObject(query);
    }


    private static boolean updateObject(String query) {
        try (Connection conn = DBConnection.INSTANCE.getConnection()) {
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            int updatedRowsNum = preparedStatement.executeUpdate();

            return updatedRowsNum == 1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static String getColumnsToUpdate(Field[] fields, Object o) {
        List<String> columns = Stream.of(fields).
                filter(f -> f.isAnnotationPresent(Column.class))
                .map(f -> {
                    f.setAccessible(true);
                    try {
                        return f.getAnnotation(Column.class).name() +
                                "=" +
                                (f.get(o) instanceof Number ? ("" + f.get(o)) : ("'" + f.get(o) + "'"));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());

        return String.join(",", columns);
    }


    private static String getIdsToUpdate(Field[] fields, Object o) {
        List<String> ids = Stream.of(fields).
                filter(f -> f.isAnnotationPresent(Id.class))
                .map(f -> {
                    f.setAccessible(true);
                    try {
                        return f.getAnnotation(Id.class).name() +
                                "=" +
                                (f.get(o) instanceof Number ? ("" + f.get(o)) : ("'" + f.get(o) + "'"));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());

        return String.join(" AND ", ids);
    }


    public static boolean delete(Object o) {
        Class<?> clazz = o.getClass();
        Table table = clazz.getAnnotation(Table.class);

        String name = table.scheme() + "." + table.name();
        Field[] fields = clazz.getDeclaredFields();

        String columnsToDelete = getColumnsToDelete(fields, o);

        String query = String.format("DELETE FROM %s\n" +
                        "WHERE %s;",
                name, columnsToDelete);

        return deleteObject(query);
    }


    private static boolean deleteObject(String query) {
        try (Connection conn = DBConnection.INSTANCE.getConnection()) {
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            int deletedRowsNum = preparedStatement.executeUpdate();

            return deletedRowsNum == 1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static String getColumnsToDelete(Field[] fields, Object o) {
        List<String> columns = Stream.of(fields).
                filter(f -> f.isAnnotationPresent(Column.class) || f.isAnnotationPresent(Id.class))
                .map(f -> {
                    f.setAccessible(true);
                    try {
                        return (f.isAnnotationPresent(Column.class) ? f.getAnnotation(Column.class).name() : f.getAnnotation(Id.class).name()) +
                                "=" +
                                (f.get(o) instanceof Number ? ("" + f.get(o)) : ("'" + f.get(o) + "'"));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());

        return String.join(" AND ", columns);
    }


    public static <E> boolean delete(Class<E> o, Object id) {
        Field[] fields = o.getDeclaredFields();

        String idToDelete = getIdToDelete(o, id, fields);

        Table table = o.getAnnotation(Table.class);
        String name = table.scheme() + "." + table.name();

        String query = String.format("DELETE FROM %s\n" +
                        "WHERE %s;",
                name, idToDelete);

        return deleteById(query);
    }


    private static boolean deleteById(String query) {
        try (Connection conn = DBConnection.INSTANCE.getConnection()) {
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            int deletedRowsNum = preparedStatement.executeUpdate();

            return deletedRowsNum == 1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static <E> String getIdToDelete(Class<E> o, Object id, Field[] fields) {
        try {
            return Stream.of(fields)
                    .filter(f -> f.isAnnotationPresent(Id.class))
                    .map(f -> f.getAnnotation(Id.class).name() +
                            "=" +
                            "" + id)
                    .findFirst()
                    .orElseThrow(() -> new IdNotFoundException("Id does not exist"));
        } catch (IdNotFoundException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }


    public static <E> List<E> select(Class<E> o) {
        List<E> result = new ArrayList<>();

        Field[] fields = o.getDeclaredFields();
        String columnNames = getSelectedColumnNames(fields);

        Table table = o.getAnnotation(Table.class);
        String name = table.scheme() + "." + table.name();

        String query = String.format("SELECT %s FROM %s", columnNames, name);

        getCollectionOfAll(query, columnNames, o, result);

        return result;
    }


    private static <E> void getCollectionOfAll(String query, String columnNames, Class<E> o, List<E> result) {
        try (Connection conn = DBConnection.INSTANCE.getConnection()) {
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            ResultSet rs = preparedStatement.executeQuery();
            String[] colNameArr = columnNames.split(",");
            Object[] paramsToPass = new Object[colNameArr.length];
            Constructor<?> constructor = getConstructor(o, colNameArr.length);

            while (rs.next()) {
                for (int i = 0; i < colNameArr.length; i++) {
                    paramsToPass[i] = rs.getObject(colNameArr[i]);
                }

                result.add((E) constructor.newInstance(paramsToPass));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static <E> Constructor<?> getConstructor(Class<E> o, int paramNum) {
        try {
            return Stream.of(o.getConstructors())
                    .filter(c -> c.getParameterCount() == paramNum)
                    .findFirst()
                    .orElseThrow(() -> new ConstructorNotFoundException("Constructor does not exist"));
        } catch (ConstructorNotFoundException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }


    private static String getSelectedColumnNames(Field[] fields) {
        List<String> columns = Stream.of(fields)
                .filter(f -> f.isAnnotationPresent(Id.class) || f.isAnnotationPresent(Column.class))
                .map(f -> f.isAnnotationPresent(Id.class) ? f.getAnnotation(Id.class).name() : f.getAnnotation(Column.class).name())
                .collect(Collectors.toList());

        return String.join(",", columns);
    }


    public static <E> E selectbyId(Class<E> o, Object id) {
        Field[] fields = o.getDeclaredFields();
        String idColumnName = getSelectedIdColumnNames(fields);
        String columnNames = getSelectedColumnNames(fields);

        Table table = o.getAnnotation(Table.class);
        String name = table.scheme() + "." + table.name();

        String query = String.format("SELECT %s FROM %s WHERE %s=%d",
                columnNames, name, idColumnName, id);

        return getSelectedById(query, columnNames, o);
    }


    private static <E> E getSelectedById(String query, String columnNames, Class<E> o) {
        E result = null;

        try (Connection conn = DBConnection.INSTANCE.getConnection()) {
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            ResultSet rs = preparedStatement.executeQuery();
            String[] colNameArr = columnNames.split(",");
            Object[] paramsToPass = new Object[colNameArr.length];
            Constructor<?> constructor = getConstructor(o, colNameArr.length);

            while (rs.next()) {
                for (int i = 0; i < colNameArr.length; i++) {
                    paramsToPass[i] = rs.getObject(colNameArr[i]);
                }

                result = (E) constructor.newInstance(paramsToPass);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }


    private static String getSelectedIdColumnNames(Field[] fields) {
        try {
            return Stream.of(fields)
                    .filter(f -> f.isAnnotationPresent(Id.class))
                    .map(f -> f.getAnnotation(Id.class).name())
                    .findFirst()
                    .orElseThrow(() -> new InvalidAnnotatedFieldException("Field with id annotation does not exist"));
        } catch (InvalidAnnotatedFieldException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
