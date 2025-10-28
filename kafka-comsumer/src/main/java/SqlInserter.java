import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SqlInserter {
    private static final String DB_URL =
            "jdbc:postgresql://192.168.118.130:5432/postgres?serverTimezone=UTC";
    private static final String USER = "gaussdb";
    private static final String PASS = "XQT@mypass123456";

    public static void insert(String machineRoom, String cabinet, int temperature, int voltage) {

        String sql = "insert into " +
                "gaussdb.server_status (machineRoom,cabinet,temperature,voltage) " +
                "values (?,?,?,?)";
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, machineRoom);
            pstmt.setString(2, cabinet);
            pstmt.setInt(3, temperature);
            pstmt.setInt(4, voltage);
            int rowsAffected = pstmt.executeUpdate();
            System.out.printf(
                    "成功插入数据：机房: %s, 机柜: %s, temperture: %d, voltage: %d。影响行数：%d\n",
                    machineRoom, cabinet, temperature, voltage, rowsAffected);
        } catch (SQLException e) {
            System.out.println("数据库操作失败" + e.getMessage());
            e.printStackTrace();
        }
    }
}
