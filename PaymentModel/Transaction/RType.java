package Transaction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import java.sql.Date;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

class RType {
	String loan_id;
	Date dueDate;
	double paid;
	Date paidDate;
	static PreparedStatement update_pstmt = null;
	static Statement stmt = null;
	static String truncate_sql = "truncate payment_paid;";
	static Statement move_stmt = null;
	static String url = null, user = "root", password = "";
	static Connection conn = null;

	public RType(String loan_id, Date dueDate, Date paidDate, double paid) {
		this.loan_id = loan_id;
		this.paidDate = paidDate;
		this.dueDate = dueDate;
		this.paid = paid;
	}

	Date getDueDate() {
		return this.dueDate;
	}

	void setDueDate(Date d) {
		this.dueDate = d;
	}

	double getPaid() {
		return this.paid;
	}

	void setPaid(double paid) {
		this.paid = paid;
	}

	Date getPaidDate() {
		return this.paidDate;
	}

	void setPaidDate(Date d) {
		this.paidDate = d;
	}

	static void writeAllMatchingIntoDB(ArrayList<RType> al) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("driver error");
			e.printStackTrace();
		}
		try {
			url = "jdbc:mysql://localhost/Payment_Model?";
			// 简单写法：url
			// ="jdbc:myqsl://localhost/test(数据库名)? user=root(用户)&password=dfg353434(密码)";
			user = "root";
			password = "";
			conn = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			System.out.println("connection error.");
			e.printStackTrace();
		}

		System.out.println("al size: " + al.size());
		SQLException YorN_exp = new SQLException(
				"The move sql from payment schedule to payment paid table has failed.");
		;
		try {
			stmt = conn.createStatement();
			stmt.execute(truncate_sql);
			String move_sql = "insert into payment_paid(loan_id, pay_period, receivable_date, receivable) "
					+ "select loan_id, pay_period, payment_date, monthly_payment from payment_schedule;";
			move_stmt = conn.createStatement();
			Boolean success = move_stmt.execute(move_sql);
			for (RType a : al) {
				java.sql.Date d = java.sql.Date.valueOf(a.dueDate.toString());
				String update_sql = "update payment_paid set paid_date=?, paid=? where loan_id=? and receivable_date=?";
				update_pstmt = conn.prepareStatement(update_sql);				
				update_pstmt.setDate(1, new java.sql.Date(a.paidDate.getTime()));
				System.out.println("updated: " + a.loan_id
						+ " with paid_amt FIRST " + a.paid);
				update_pstmt.setDouble(2, a.paid);
				update_pstmt.setString(3, a.loan_id);
				update_pstmt.setDate(4, (java.sql.Date) a.dueDate);
				int rc = update_pstmt.executeUpdate();
				System.out.println("updated: " + a.loan_id + " with paid_amt "
						+ a.paid + " dueDate: " + a.dueDate + " paidDate: "
						+ a.paidDate + " and return code: " + rc);
			}
		} catch (SQLException e) {
			System.out.println("While updating payment paid table....");
			e.printStackTrace();
		}
	}

}