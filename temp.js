db.restaurant.aggregate([
  { $unwind: "$grades" },
  { $match: { "grades.score": { $gt: 90 } } },
  { $group: { _id: "$borough", highScoreCount: { $sum: 1 } } },
  {
    $lookup: {
      from: "restaurant",
      localField: "_id",
      foreignField: "borough",
      pipeline: [{ $group: { _id: null, totalCount: { $sum: 1 } } }],
      as: "totalRestaurants",
    },
  },
  { $unwind: "$totalRestaurants" },
  {
    $project: {
      _id: 0,
      borough: "$_id",
      percentage: {
        $multiply: [
          { $divide: ["$highScoreCount", "$totalRestaurants.totalCount"] },
          100,
        ],
      },
    },
  },
]);

db.restaurant.aggregate([
  // First, group all restaurants by borough to get total counts
  {
    $group: {
      _id: "$borough",
      totalCount: { $sum: 1 },
      restaurants: { $push: "$$ROOT" },
    },
  },
  // Unwind the restaurants array
  { $unwind: "$restaurants" },
  // Unwind the grades for each restaurant
  { $unwind: "$restaurants.grades" },
  // Group again to count high scores
  {
    $group: {
      _id: "$_id",
      totalCount: "$totalCount",
      highScoreCount: {
        $sum: { $cond: [{ $gt: ["$restaurants.grades.score", 90] }, 1, 0] },
      },
    },
  },
  // Calculate the percentage
  {
    $project: {
      _id: 0,
      borough: "$_id",
      percentage: {
        $multiply: [
          { $divide: [{ $ifNull: ["$highScoreCount", 0] }, "$totalCount"] },
          100,
        ],
      },
    },
  },
  // Sort by percentage descending
  { $sort: { percentage: -1 } },
]);

db.restaurant.aggregate([
  // First, group all restaurants by borough to get total counts
  {
    $group: {
      _id: "$borough",
      totalCount: { $sum: 1 },
    },
  },
]);
