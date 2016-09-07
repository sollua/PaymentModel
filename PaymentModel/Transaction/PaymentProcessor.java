package Transaction;

import java.sql.Connection;
import java.util.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.Calendar;

import com.mysql.jdbc.CallableStatement;

public class PaymentProcessor {

	private static double PMTClassic(double rate, double term, double financeAmount) {
		double v = (1 + (rate / 12));
		double t = - term;
		double result = (financeAmount * (rate / 12)) / (1 - Math.pow(v, t));
		return result;
	}
	private static double interest(double rate, double term, double thisInstallment, double financeAmount) {
		double v = (1 + (rate / 12));
		double noOfInstallments = term;
		double result = financeAmount * (rate / 12)* (1 - Math.pow(v, thisInstallment-1-noOfInstallments))
				/ (1 - Math.pow(v, -noOfInstallments));
		return result;
	}
	
	private static double principlePaydown(double rate, double term, double thisInstallment, double financeAmount) {
		double v = (1 + (rate / 12));
		double noOfInstallments = term;
		double result = financeAmount * (rate / 12)* Math.pow(v, thisInstallment-1)
				/ (Math.pow(v, noOfInstallments)-1);
		return result;
	}
	
	private static double beginningBalance(double rate, double term, double thisInstallment, double financeAmount) {
		double v = (1 + (rate / 12));
		double noOfInstallments = term;
		double result = financeAmount * (1 - Math.pow(v, thisInstallment-1-noOfInstallments))
				/ (1 - Math.pow(v, -noOfInstallments));
		return result;
	}
	
	private static double endingBalance(double rate, double term, double thisInstallment, double financeAmount) {
		double v = (1 + (rate / 12));
		double noOfInstallments = term;
		double result = financeAmount * (1 - Math.pow(v, thisInstallment-noOfInstallments))
				/ (1 - Math.pow(v, -noOfInstallments));
		return result;
	}
	
	
	static void paymentScheduleOutLaying(){
		//generating payment schedule onto payment_schedule table, according to payment type input. truncate payment_schedule each time in advance.
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		PreparedStatement update_pstmt = null;
		ResultSet rs = null;
		String url = null;
		String user = null;
		String password = null;
		String sql = null;
		Calendar cal = null;
		double monthly_pmt;
		double interest = 0.0;
		double principlePaydown = 0.0;
		double endingBalance = 0.0;
		double beginningBalance = 0.0;
		String truncate_sql = null;
		Boolean newBatchUpdate = true;
		final int batchSize = 1000;
		int count = 0;
		DecimalFormat twoDForm = new DecimalFormat("#.##");

		try {
			Class.forName("com.mysql.jdbc.Driver"); //
		} catch (ClassNotFoundException e) {
			System.out.println("driver error");
			e.printStackTrace();
		}
		try {		
			// 简单写法：url =
			// "jdbc:myqsl://localhost/test(数据库名)? user=root(用户)&password=dfg353434(密码)";
			url =
			// "jdbc:mysql://localhost/delinq_db?useUnicode=true&&characterEncoding=gb2312&autoReconnect = true";
			"jdbc:mysql://localhost/Payment_Model?profileSQL=false&&"
			+ "traceProtocol=false&&useUnicode=true&&characterEncoding=gb2312&autoReconnect = true";
			user = "root";
			password = "";
			conn = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			System.out.println("connection error.");
			e.printStackTrace();
		}
		try {
			sql = "select loan_No, amortization_type, installments, loan_amt, annual_rate, fst_payment_date from loan";
			String update_sql = "insert into payment_schedule values(?,?,?,?,?,?,?,?)";
			pstmt = conn.prepareStatement(sql);
			truncate_sql = "truncate payment_schedule;";
			stmt = conn.createStatement();
			stmt.execute(truncate_sql);
			update_pstmt = conn.prepareStatement(update_sql);
			conn.setAutoCommit(false);
			rs = pstmt.executeQuery();
			cal = Calendar.getInstance();

			while (rs.next()) {
				String loan_No = rs.getString("loan_No");
				String amortization_type = rs.getString("amortization_type");
				double loan_amt = rs.getDouble("loan_amt");
				double annual_rate = rs.getDouble("annual_rate")/100;
				//int payment_date = rs.getInt("payment_date");
				int installments = rs.getInt("installments");
				Date fst_pyment_date = rs.getDate("fst_payment_date");
				Date p_date = fst_pyment_date;
				Date fst_payment_date = fst_pyment_date;
				cal.setTime(p_date);
				//cal.set(Calendar.DAY_OF_MONTH, payment_date);
				cal.add(Calendar.MONTH, -1);
				p_date = cal.getTime();
				System.out.println("the loan_No is " + loan_No);
				System.out.println("the loan_amt is " + loan_amt);
				System.out.println("the fst_pyment_date is " + fst_pyment_date);
				System.out.println("the installments is " + installments);
				System.out.println("the fst_pyment_date is " +
				  fst_pyment_date);
				monthly_pmt = PMTClassic(annual_rate, installments, loan_amt);
				beginningBalance = loan_amt;
				for (int i = 0; i < installments; i++) {
					if (newBatchUpdate == false) {
						beginningBalance = endingBalance;
					}
					//cal.setTime(p_date);
					//cal.add(Calendar.MONTH, 1);
					cal.setTime(fst_pyment_date);
					cal.add(Calendar.MONTH, i);
					p_date = cal.getTime();
					if (amortization_type.equals("等额本息"))// 等额本息
					{
						//debated method of interest calculation:
						//interest = beginningBalance * annual_rate / 12;
						interest = interest(annual_rate, installments, i+1 , loan_amt);
						//debated method of principlePaydown calculation:
						//principlePaydown = monthly_pmt - interest;
						principlePaydown = principlePaydown(annual_rate, installments, i+1 , loan_amt);
						//debated method of beginningBalance calculation:
						beginningBalance = beginningBalance(annual_rate, installments, i+1 , loan_amt);
						//debated method of endingBalance calculation:
						endingBalance = endingBalance(annual_rate, installments, i+1 , loan_amt);
					} else if (amortization_type.equals("先息后本")) {// 先息后本
						interest = beginningBalance * annual_rate / 12;
						principlePaydown = 0;
						monthly_pmt = interest;
						endingBalance = loan_amt;
						beginningBalance = loan_amt;
						if (i == installments - 1) {
							monthly_pmt = loan_amt + interest;
							principlePaydown = loan_amt;
							endingBalance = 0;
							beginningBalance = loan_amt;
						}
					} else if (amortization_type.equals("到期还本付息")) { // 到期还本付息
						// interest= beginningBalance*annual_rate/12;
						interest = 0;
						monthly_pmt = 0;
						principlePaydown = monthly_pmt - interest;
						endingBalance = loan_amt;
						beginningBalance = loan_amt;
						if (i == installments - 1) {
							monthly_pmt = loan_amt + beginningBalance
									* annual_rate / 12 * installments;
							interest = beginningBalance * annual_rate / 12
									* installments;
							beginningBalance = loan_amt;
							endingBalance = 0;
						}
					} else {
						interest = beginningBalance * annual_rate / 12;
						principlePaydown = monthly_pmt - interest;
						endingBalance = beginningBalance - principlePaydown;
					}
					update_pstmt.setString(1, loan_No);
					update_pstmt.setInt(2, i+1);
					java.sql.Date d = new java.sql.Date(p_date.getTime());
					//@SuppressWarnings("deprecation")
					//java.sql.Date dummy_1999_Halloween = new java.sql.Date(99,10,01);
					update_pstmt.setDate(3, d);					
					update_pstmt.setDouble(4,
							Double.valueOf(twoDForm.format(beginningBalance)));
					update_pstmt.setDouble(5,
							Double.valueOf(twoDForm.format(interest)));
					update_pstmt.setDouble(6,
							Double.valueOf(twoDForm.format(principlePaydown)));	
					update_pstmt.setDouble(7, Math.round(monthly_pmt));
					update_pstmt.setDouble(8,
							Double.valueOf(twoDForm.format(endingBalance)));					
					//update_pstmt.setDate(9, dummy_1999_Halloween);					
					//update_pstmt.setDouble(10, 0.0);					
					update_pstmt.addBatch();
					if (i == installments - 1) {
						newBatchUpdate = true;
					} else {
						newBatchUpdate = false;
					}
					// Create an int[] to hold returned values
					if (++count % batchSize == 0) {
						System.out.println("executed up to: "+ count);
						int[] cnt = update_pstmt.executeBatch();
					}
					// Explicitly commit statements to apply changes
					conn.commit();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				update_pstmt.executeBatch();
				conn.commit();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			if (rs != null) {
				rs.close();
				rs = null;
			}
			if (update_pstmt != null) {
				update_pstmt.close();
				update_pstmt = null;
			}
			if (conn != null) {
				conn.close();
				conn = null;
			}
		} catch (Exception e) {
			System.out.println("error on closing.");
			e.printStackTrace();
		}
	}
	

	// beginning of calculating fulfillment rate logic
	static void fulfillmentRate(Date beginningRunDate,
			Date endingRunDate) {
		Calendar cal = Calendar.getInstance();
		double totalReceivable = 0.0, totalPayment = 0.0, fulfillmentRate = 0.0;
		PreparedStatement pstmt_schedule = null, pstmt_payment = null, pstmt_risk_index = null;
		Connection conn = null;
		ResultSet rs = null;
		String url = null;
		String user = null;
		String password = null;
		java.sql.Date sqlBeginningRunDate = new java.sql.Date(
				beginningRunDate.getTime());
		java.sql.Date sqlEndingRunDate = new java.sql.Date(
				endingRunDate.getTime());
		java.sql.Date sqlRunDate = sqlBeginningRunDate;
		cal.setTime(beginningRunDate);
		
		try {
				Class.forName("com.mysql.jdbc.Driver"); //
			} catch (ClassNotFoundException e) {
				System.out.println("driver error");
				e.printStackTrace();
			}
			try {
				url =
				// "jdbc:mysql://localhost/delinq_db?useUnicode=true&&characterEncoding=gb2312&autoReconnect = true";
				"jdbc:mysql://localhost/Payment_Model?profileSQL=false&&"
						+ "traceProtocol=false&&useUnicode=true&&characterEncoding=gb2312&autoReconnect = true";
				// 简单写法：url =
				// "jdbc:myqsl://localhost/test(数据库名)? user=root(用户)&password=dfg35(密码)";
				user = "root";
				password = "";
				conn = DriverManager.getConnection(url, user, password);
			} catch (SQLException e) {
				System.out.println("connection error.");
				e.printStackTrace();
			}
		while (sqlRunDate.before(sqlEndingRunDate)
				|| sqlRunDate.equals(sqlEndingRunDate)) {
			totalReceivable = 0.0; totalPayment = 0.0; fulfillmentRate = 0.0;
			try {
				String sql_schedule = "select loan_id, monthly_payment, payment_date from payment_schedule where payment_date<= ?;";
				String sql_payment = "select pay_amt, pay_date from payment where pay_date<= ?;";
				//String update_sql = " historical_risk_index (runDate, fulfillment_rate) values(?,?)";				
				String update_sql ="insert into historical_risk_index (runDate, fulfillment_rate) VALUES(?, ?) on duplicate key update fulfillment_rate=?";
				pstmt_schedule = conn.prepareStatement(sql_schedule);
				pstmt_schedule.setDate(1, sqlRunDate);
				pstmt_payment = conn.prepareStatement(sql_payment);
				pstmt_payment.setDate(1, sqlRunDate);
				rs = pstmt_schedule.executeQuery();
				pstmt_risk_index = conn.prepareStatement(update_sql);
				while (rs.next()) {
					double monthlyPayment = rs.getDouble("monthly_payment");
					Date payment_date = rs.getDate("payment_date");
					totalReceivable += monthlyPayment;
				}
				rs = null;
				rs = pstmt_payment.executeQuery();
				while (rs.next()) {
					double payAmt = rs.getDouble("pay_amt");
					Date payDate = rs.getDate("pay_date");
					totalPayment += payAmt;
				}
				fulfillmentRate = (totalReceivable - totalPayment)
						/ totalReceivable;
				pstmt_risk_index.setDate(1, sqlRunDate);
				pstmt_risk_index.setDouble(2, fulfillmentRate);
				pstmt_risk_index.setDouble(3, fulfillmentRate);
				pstmt_risk_index.execute();
				cal.add(Calendar.DATE, 1);
				sqlRunDate = new java.sql.Date(cal.getTime().getTime());
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
			}
		}
		return;
	}
	
	public static void main(String[] args) {
		//Step 1, paymentScheduleOutLaying()
		paymentScheduleOutLaying();
		@SuppressWarnings("deprecation")
		Date beginningRundate=new Date("10/18/2015");
		Date endingRunDate=new Date("12/31/2015");
		//step 2, fulfillmentRate()
		fulfillmentRate(beginningRundate,endingRunDate);
	}
}
