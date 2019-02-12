class DropSomeTable < ActiveRecord::Migration[5.0]
  def up
    drop_table :some
  end
end
