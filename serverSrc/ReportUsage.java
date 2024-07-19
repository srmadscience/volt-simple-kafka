/* This file is part of VoltDB.
 * Copyright (C) 2008-2024 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.util.Date;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class ReportUsage extends VoltProcedure {

    public static final SQLStmt checkDriver = new SQLStmt("SELECT * FROM drivers WHERE driver_name = ?;");

    public static final SQLStmt checkVehicle = new SQLStmt("SELECT * FROM vehicles WHERE vin = ?;");

    public static final SQLStmt checkUsage = new SQLStmt("SELECT * FROM driver_car_use " + "WHERE vin = ? "
            + "AND driver_name = ? " + "AND NOW BETWEEN start_time AND end_time;");

    public static final SQLStmt endOtherUsage = new SQLStmt("UPDATE driver_car_use " + "SET end_time = NOW "
            + "WHERE vin != ? " + "AND driver_name = ? " + "AND end_time > NOW;");

    public static final SQLStmt reportNaughtyness = new SQLStmt("INSERT INTO naughty_driver_stream "
            + "(driver_name, vin, observed_date, driver_max_speed, drive_max_vehicles_24_hours, message_comment) "
            + "VALUES (?,?,NOW,?,?,?);");

    public static final SQLStmt startRental = new SQLStmt(
            "INSERT INTO driver_car_use (driver_name,vin,start_time,end_time, status, max_speed) "
                    + "VALUES (?,?,NOW,?,'START',?);");

    public static final SQLStmt updateRental = new SQLStmt(
            "UPDATE driver_car_use  SET max_speed = ?,status = ? WHERE driver_name = ? AND vin = ? AND end_time = ?;");

    public static final SQLStmt endRental = new SQLStmt(
            "UPDATE driver_car_use  SET max_speed = ?,status = ?, end_time = NOW WHERE driver_name = ? AND vin = ? AND end_time = ?;");

    /**
     * Find drivers who either are speeding and either completed a rental this hour,
     * or have an ongoing rental
     */
    public static final SQLStmt checkSpeeding = new SQLStmt("select * from driver_activity_hour "
            + "WHERE driver_name = ? AND (end_time = TRUNCATE(hour, NOW) OR end_time > NOW) AND max_speed > ?;");

    @SuppressWarnings("deprecation")
    public static final TimestampType THE_FUTURE = new TimestampType(new Date(1124, 1, 1, 1, 04));

    /**
     * 
     * Track usage of a shared car
     * 
     * @param driverName
     * @param vin
     * @param reportedSpeed
     * @param status
     * @return
     * @throws VoltAbortException
     */
    @SuppressWarnings("deprecation")
    public VoltTable[] run(String driverName, String vin, int reportedSpeed, String status) throws VoltAbortException {

        // Queue SQL statements to see
        // * Does driver exist?
        // * Does vehicle exist?
        // * How fast has the driver been going?
        voltQueueSQL(checkDriver, driverName);
        voltQueueSQL(checkVehicle, vin);
        voltQueueSQL(checkUsage, vin, driverName);

        // Will have 3 elements, one for each of the SQL statements above...
        VoltTable[] results = voltExecuteSQL();

        // Did our driver query match a row?
        if (!results[0].advanceRow()) {

            reportProblem(driverName, vin, -1, -1, "No Driver Found");
            return voltExecuteSQL(true);

        }

        // Get max speed this driver is allowed to go...
        final long driverMaxAllowedSpeed = results[0].getLong("driver_max_speed");
        final long driverMaxAllowedVehicles = results[0].getLong("driver_max_vehicles_24_hours");

        // Did our vehicle query match a row?
        if (!results[1].advanceRow()) {

            reportProblem(driverName, vin, -1, -1, "No Vehicle Found");
            return voltExecuteSQL(true);

        }

        // Is there an ongoing rental?
        if (!results[2].advanceRow()) {

            // New rental...
            voltQueueSQL(startRental, driverName, vin, THE_FUTURE, reportedSpeed);

        } else {

            // See how fast we are driving, and has our max speed gone up?
            long maxSpeed = results[2].getLong("max_speed");

            if (reportedSpeed > maxSpeed) {
                maxSpeed = reportedSpeed;
            }

            if (status.equalsIgnoreCase("END")) {
                // End rental...
                voltQueueSQL(endRental, maxSpeed, "Ended " + this.getTransactionTime().toGMTString(), driverName, vin,
                        THE_FUTURE);

            } else {
                // Update rental...
                voltQueueSQL(updateRental, maxSpeed, "Updated " + this.getTransactionTime().toGMTString(), driverName,
                        vin, THE_FUTURE);

            }
        }

        // Now see if we need to add this person to the naughty list...
        voltQueueSQL(checkSpeeding, driverName, driverMaxAllowedSpeed);

        VoltTable speedHistory = voltExecuteSQL()[1];

        if (speedHistory.advanceRow()) {
            long actualMaxSpeed = speedHistory.getLong("max_speed");

            if (reportedSpeed == actualMaxSpeed) {
                reportProblem(driverName, vin, actualMaxSpeed, -1,
                        "Limited to " + driverMaxAllowedSpeed + " but drove at " + reportedSpeed);

            } else {
                reportProblem(driverName, vin, actualMaxSpeed, -1, "Limited to " + driverMaxAllowedSpeed
                        + " but drove at " + reportedSpeed + " and has driven at " + actualMaxSpeed);

            }

        }

        return voltExecuteSQL(true);

    }

    private void reportProblem(String driverName, String vin, long actualMaxSpeed, long maxVehicles, String message) {
        voltQueueSQL(reportNaughtyness, driverName, vin, actualMaxSpeed, maxVehicles, message);

    }

}
